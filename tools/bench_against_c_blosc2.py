#!/usr/bin/env python3
"""Benchmark blosc2-pure-rs against original C-Blosc2 on real files.

The harness measures process wall time and max RSS via /usr/bin/time, verifies
roundtrips, and writes CSV plus a short Markdown summary.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 fallback.
    tomllib = None


REPO = Path(__file__).resolve().parents[1]
TIME_BIN = Path("/usr/bin/time")


@dataclass(frozen=True)
class Dataset:
    name: str
    path: Path
    typesizes: tuple[int, ...]


@dataclass(frozen=True)
class Case:
    codec: str
    clevel: int
    filter: str
    filter_meta: int
    typesize: int
    nthreads: int
    chunksize: int
    blocksize: int
    splitmode: str
    use_dict: bool = False


@dataclass
class RunResult:
    status: str
    wall_s: float | None
    max_rss_kb: int | None
    stdout: str
    stderr: str
    returncode: int


def run(cmd: list[str], cwd: Path = REPO, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(cmd, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check and result.returncode != 0:
        raise SystemExit(
            f"command failed ({result.returncode}): {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def timed_run(cmd: list[str], cwd: Path = REPO) -> RunResult:
    if not TIME_BIN.exists():
        raise SystemExit("/usr/bin/time is required for RSS measurement")
    wrapped = [str(TIME_BIN), "-f", "BENCH_TIME\t%e\t%M", *cmd]
    result = subprocess.run(wrapped, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    wall_s = None
    max_rss_kb = None
    stderr_lines = []
    for line in result.stderr.splitlines():
        if line.startswith("BENCH_TIME\t"):
            _, wall, rss = line.split("\t", 2)
            wall_s = float(wall)
            max_rss_kb = int(rss)
        else:
            stderr_lines.append(line)
    status = "ok" if result.returncode == 0 else f"exit_{result.returncode}"
    return RunResult(status, wall_s, max_rss_kb, result.stdout, "\n".join(stderr_lines), result.returncode)


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest(path: Path) -> list[Dataset]:
    if tomllib is None:
        raise SystemExit("TOML manifests require Python 3.11+")
    data = tomllib.loads(path.read_text())
    datasets = []
    for raw in data.get("dataset", []):
        dataset_path = Path(raw["path"])
        if not dataset_path.is_absolute():
            dataset_path = (REPO / dataset_path).resolve()
        datasets.append(
            Dataset(
                name=str(raw["name"]),
                path=dataset_path,
                typesizes=tuple(int(v) for v in raw.get("typesizes", [1])),
            )
        )
    if not datasets:
        raise SystemExit(f"manifest has no [[dataset]] entries: {path}")
    return datasets


def discover_datasets(fixtures_dir: Path) -> list[Dataset]:
    datasets = []
    for path in sorted(p for p in fixtures_dir.rglob("*") if p.is_file()):
        if path.name.startswith("."):
            continue
        datasets.append(Dataset(path.stem.replace(".", "_"), path.resolve(), (1,)))
    if not datasets:
        raise SystemExit(f"no fixture files found under {fixtures_dir}")
    return datasets


def cases_for_profile(profile: str, typesizes: Iterable[int]) -> list[Case]:
    if profile == "quick":
        codecs = ["blosclz", "lz4", "zstd"]
        clevels = [5]
        filters = [("nofilter", 0), ("shuffle", 0)]
        threads = [1, 4]
        splitmodes = ["forward"]
        chunksizes = [1_000_000]
    elif profile == "publish":
        codecs = ["blosclz", "lz4", "lz4hc", "zlib", "zstd"]
        clevels = [1, 5, 9]
        filters = [("nofilter", 0), ("shuffle", 0), ("bitshuffle", 0), ("delta", 0), ("truncprec", 16)]
        threads = [1, 4]
        splitmodes = ["forward", "never"]
        chunksizes = [1_000_000, 4_194_304]
    elif profile == "full":
        codecs = ["blosclz", "lz4", "lz4hc", "zlib", "zstd"]
        clevels = [1, 3, 5, 7, 9]
        filters = [("nofilter", 0), ("shuffle", 0), ("bitshuffle", 0), ("delta", 0), ("truncprec", 16)]
        threads = [1, 2, 4, 8]
        splitmodes = ["always", "never", "auto", "forward"]
        chunksizes = [262_144, 1_000_000, 4_194_304]
    else:
        raise SystemExit(f"unknown profile: {profile}")

    out = []
    dict_codecs = {"lz4", "lz4hc", "zstd"}
    for typesize in typesizes:
        for codec in codecs:
            for clevel in clevels:
                for filter_name, filter_meta in filters:
                    if filter_name in {"bitshuffle", "delta", "truncprec"} and typesize <= 1:
                        continue
                    use_dict_options = [False]
                    if codec in dict_codecs and clevel > 0:
                        if profile == "quick":
                            if filter_name == "shuffle":
                                use_dict_options.append(True)
                        else:
                            use_dict_options.append(True)
                    for nthreads in threads:
                        for splitmode in splitmodes:
                            for chunksize in chunksizes:
                                for use_dict in use_dict_options:
                                    out.append(
                                        Case(
                                            codec=codec,
                                            clevel=clevel,
                                            filter=filter_name,
                                            filter_meta=filter_meta,
                                            typesize=typesize,
                                            nthreads=nthreads,
                                            chunksize=chunksize,
                                            blocksize=0,
                                            splitmode=splitmode,
                                            use_dict=use_dict,
                                        )
                                    )
    return out


def build_rust(args: argparse.Namespace) -> Path:
    if args.rust_bin:
        return Path(args.rust_bin).resolve()
    run(["cargo", "build", "--release", "--features", "cli"])
    return REPO / "target/release/blosc2"


def build_c_helper(args: argparse.Namespace) -> Path:
    if args.c_helper:
        return Path(args.c_helper).resolve()

    output = REPO / "target/real-bench/c_blosc2_file_bench"
    source = REPO / "tools/c_blosc2_file_bench.c"
    include = REPO / "c-blosc2/include"
    lib, generated_include = ensure_c_blosc2_static_lib()
    if not include.exists():
        raise SystemExit("missing c-blosc2/include; pass --c-helper PATH or restore vendored C-Blosc2")
    output.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "gcc",
        "-O3",
        "-DNDEBUG",
        "-I",
        str(include),
        "-I",
        str(generated_include),
        str(source),
        str(lib),
        "-lm",
        "-lpthread",
        "-ldl",
        "-o",
        str(output),
    ]
    run(cmd)
    return output


def ensure_c_blosc2_static_lib() -> tuple[Path, Path]:
    checked_in = REPO / "c-blosc2/build-ref/blosc/libblosc2.a"
    checked_in_generated = REPO / "c-blosc2/build-ref/blosc"
    if checked_in.exists():
        return checked_in, checked_in_generated

    source_dir = REPO / "c-blosc2"
    if not source_dir.exists():
        raise SystemExit("missing c-blosc2 source tree; pass --c-helper PATH")
    build_dir = REPO / "target/real-bench/c-blosc2-build"
    build_dir.mkdir(parents=True, exist_ok=True)
    run(
        [
            "cmake",
            "-S",
            str(source_dir),
            "-B",
            str(build_dir),
            "-DBUILD_TESTS=OFF",
            "-DBUILD_FUZZERS=OFF",
            "-DBUILD_BENCHMARKS=OFF",
            "-DBUILD_EXAMPLES=OFF",
            "-DBUILD_SHARED=OFF",
            "-DBUILD_STATIC=ON",
            "-DBUILD_PLUGINS=ON",
            "-DDEACTIVATE_IPP=ON",
            "-DPREFER_EXTERNAL_LZ4=OFF",
            "-DPREFER_EXTERNAL_ZLIB=OFF",
            "-DPREFER_EXTERNAL_ZSTD=OFF",
        ]
    )
    run(["cmake", "--build", str(build_dir), "--target", "blosc2_static", "-j"])
    candidates = [
        build_dir / "blosc/libblosc2.a",
        build_dir / "lib/libblosc2.a",
        build_dir / "lib64/libblosc2.a",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate, build_dir / "blosc"
    raise SystemExit(f"failed to locate built C-Blosc2 static library under {build_dir}")


def common_options(case: Case) -> list[str]:
    options = [
        "--codec",
        case.codec,
        "--clevel",
        str(case.clevel),
        "--typesize",
        str(case.typesize),
        "--blocksize",
        str(case.blocksize),
        "--chunksize",
        str(case.chunksize),
        "--splitmode",
        case.splitmode,
        "--nthreads",
        str(case.nthreads),
        "--filter",
        case.filter,
        "--filter-meta",
        str(case.filter_meta),
    ]
    if case.use_dict:
        options.append("--use-dict")
    return options


def rust_compress_cmd(rust_bin: Path, dataset: Path, output: Path, case: Case) -> list[str]:
    return [str(rust_bin), "compress", str(dataset), str(output), *common_options(case)]


def rust_decompress_cmd(rust_bin: Path, compressed: Path, output: Path, case: Case) -> list[str]:
    return [str(rust_bin), "decompress", str(compressed), str(output), "--nthreads", str(case.nthreads)]


def c_compress_cmd(c_helper: Path, dataset: Path, output: Path, case: Case) -> list[str]:
    return [str(c_helper), "compress", str(dataset), str(output), *common_options(case)]


def c_decompress_cmd(c_helper: Path, compressed: Path, output: Path, case: Case) -> list[str]:
    return [str(c_helper), "decompress", str(compressed), str(output), "--nthreads", str(case.nthreads)]


def add_row(
    rows: list[dict[str, object]],
    dataset: Dataset,
    case: Case,
    impl: str,
    mode: str,
    frame_impl: str,
    input_bytes: int,
    output_bytes: int | None,
    result: RunResult,
    verified: bool | str | None,
) -> None:
    mbps = None
    if result.wall_s and result.wall_s > 0:
        mbps = input_bytes / result.wall_s / (1024 * 1024)
    ratio = None
    if output_bytes and output_bytes > 0:
        ratio = input_bytes / output_bytes
    rows.append(
        {
            "dataset": dataset.name,
            "dataset_path": str(dataset.path),
            "impl": impl,
            "mode": mode,
            "frame_impl": frame_impl,
            "codec": case.codec,
            "clevel": case.clevel,
            "filter": case.filter,
            "filter_meta": case.filter_meta,
            "typesize": case.typesize,
            "nthreads": case.nthreads,
            "chunksize": case.chunksize,
            "blocksize": case.blocksize,
            "splitmode": case.splitmode,
            "use_dict": case.use_dict,
            "input_bytes": input_bytes,
            "output_bytes": output_bytes,
            "ratio": ratio,
            "wall_s": result.wall_s,
            "max_rss_kb": result.max_rss_kb,
            "mbps": mbps,
            "verified": verified,
            "status": result.status,
            "failure_class": classify_failure(impl, mode, frame_impl, case, result, verified),
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    )


def classify_failure(
    impl: str,
    mode: str,
    frame_impl: str,
    case: Case,
    result: RunResult,
    verified: bool | str | None,
) -> str:
    if result.status == "ok" and verified not in (False, "False", "false"):
        return ""
    if mode == "decompress" and frame_impl == "c" and case.use_dict:
        if impl == "rust" and case.codec == "zstd":
            return "rust_c_trained_zstd_dict_gap"
        return "c_reference_dict_frame_rejected"
    if mode == "decompress" and frame_impl == "rust" and case.use_dict:
        return "rust_dict_frame_rejected_by_c"
    if mode == "decompress" and verified in (False, "False", "false"):
        return "roundtrip_mismatch"
    return "process_failure"


def run_case(
    rust_bin: Path,
    c_helper: Path,
    dataset: Dataset,
    case: Case,
    workdir: Path,
    rows: list[dict[str, object]],
) -> None:
    input_hash = file_hash(dataset.path)
    input_bytes = dataset.path.stat().st_size

    rust_frame = workdir / "rust.b2frame"
    rust_out = workdir / "rust.out"
    rust_from_c_out = workdir / "rust_from_c.out"
    c_frame = workdir / "c.b2frame"
    c_out = workdir / "c.out"
    c_from_rust_out = workdir / "c_from_rust.out"

    rust_c = timed_run(rust_compress_cmd(rust_bin, dataset.path, rust_frame, case))
    rust_frame_bytes = rust_frame.stat().st_size if rust_frame.exists() else None
    add_row(rows, dataset, case, "rust", "compress", "self", input_bytes, rust_frame_bytes, rust_c, None)

    rust_d = timed_run(rust_decompress_cmd(rust_bin, rust_frame, rust_out, case))
    rust_verified = verify_decompressed("rust", dataset.path, case, rust_out, input_hash)
    add_row(rows, dataset, case, "rust", "decompress", "rust", input_bytes, rust_frame_bytes, rust_d, rust_verified)

    c_c = timed_run(c_compress_cmd(c_helper, dataset.path, c_frame, case))
    c_frame_bytes = c_frame.stat().st_size if c_frame.exists() else None
    add_row(rows, dataset, case, "c", "compress", "self", input_bytes, c_frame_bytes, c_c, None)

    c_d = timed_run(c_decompress_cmd(c_helper, c_frame, c_out, case))
    c_verified = verify_decompressed("c", dataset.path, case, c_out, input_hash)
    add_row(rows, dataset, case, "c", "decompress", "c", input_bytes, c_frame_bytes, c_d, c_verified)

    if successful_roundtrip(c_d, c_verified):
        rust_from_c = timed_run(rust_decompress_cmd(rust_bin, c_frame, rust_from_c_out, case))
        rust_from_c_verified = verify_decompressed("rust", dataset.path, case, rust_from_c_out, input_hash)
        add_row(
            rows,
            dataset,
            case,
            "rust",
            "decompress",
            "c",
            input_bytes,
            c_frame_bytes,
            rust_from_c,
            rust_from_c_verified,
        )

    c_from_rust = timed_run(c_decompress_cmd(c_helper, rust_frame, c_from_rust_out, case))
    c_from_rust_verified = verify_decompressed("c", dataset.path, case, c_from_rust_out, input_hash)
    add_row(
        rows,
        dataset,
        case,
        "c",
        "decompress",
        "rust",
        input_bytes,
        rust_frame_bytes,
        c_from_rust,
        c_from_rust_verified,
    )


def verify_decompressed(impl: str, input_path: Path, case: Case, output: Path, input_hash: str) -> bool | str:
    if case.filter == "truncprec":
        return "lossy"
    if not output.exists():
        return False
    if file_hash(output) == input_hash:
        return True
    if impl == "c" and case.filter == "delta" and input_path.stat().st_size % case.typesize != 0:
        if same_except_partial_type_tail(input_path, output, case.typesize):
            return "c_delta_tail"
    return False


def successful_roundtrip(result: RunResult, verified: bool | str | None) -> bool:
    return result.status == "ok" and verified not in (False, "False", "false", None)


def same_except_partial_type_tail(input_path: Path, output: Path, typesize: int) -> bool:
    input_size = input_path.stat().st_size
    output_size = output.stat().st_size
    tail = input_size % typesize
    if tail == 0 or input_size != output_size:
        return False
    compare_len = input_size - tail
    with input_path.open("rb") as left, output.open("rb") as right:
        remaining = compare_len
        while remaining > 0:
            chunk_size = min(1024 * 1024, remaining)
            if left.read(chunk_size) != right.read(chunk_size):
                return False
            remaining -= chunk_size
    return True


def write_outputs(rows: list[dict[str, object]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "results.csv"
    fieldnames = list(rows[0].keys()) if rows else []
    with csv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    pairs = {}
    for row in rows:
        key = tuple(row[k] for k in [
            "dataset", "mode", "frame_impl", "codec", "clevel", "filter", "filter_meta",
            "typesize", "nthreads", "chunksize", "blocksize", "splitmode", "use_dict",
        ])
        pairs.setdefault(key, {})[row["impl"]] = row

    summary_lines = [
        "# Rust vs C-Blosc2 Benchmark Summary",
        "",
        f"Rows: {len(rows)}",
        "",
        "| dataset | mode | frame | codec | filter | dict | typesize | threads | rust_s | c_s | speedup | rust_rss_kb | c_rss_kb | rss_ratio | size_ratio | status |",
        "|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for key, impls in sorted(pairs.items()):
        if "rust" not in impls or "c" not in impls:
            continue
        rust = impls["rust"]
        c = impls["c"]
        rust_s = rust["wall_s"]
        c_s = c["wall_s"]
        speedup = (
            c_s / rust_s
            if isinstance(rust_s, float) and isinstance(c_s, float) and rust_s > 0 and c_s > 0
            else None
        )
        rust_rss = rust["max_rss_kb"]
        c_rss = c["max_rss_kb"]
        rss_ratio = (rust_rss / c_rss) if isinstance(rust_rss, int) and isinstance(c_rss, int) and c_rss > 0 else None
        rust_size = rust["output_bytes"]
        c_size = c["output_bytes"]
        size_ratio = (rust_size / c_size) if isinstance(rust_size, int) and isinstance(c_size, int) and c_size > 0 else None
        status = f"{rust['status']}/{c['status']}"
        summary_lines.append(
            "| {dataset} | {mode} | {frame} | {codec} | {filter} | {dict} | {typesize} | {threads} | {rust_s} | {c_s} | {speedup} | {rust_rss} | {c_rss} | {rss_ratio} | {size_ratio} | {status} |".format(
                dataset=rust["dataset"],
                mode=rust["mode"],
                frame=rust["frame_impl"],
                codec=rust["codec"],
                filter=rust["filter"],
                dict="yes" if rust["use_dict"] else "no",
                typesize=rust["typesize"],
                threads=rust["nthreads"],
                rust_s=f"{rust_s:.4g}" if isinstance(rust_s, float) else "",
                c_s=f"{c_s:.4g}" if isinstance(c_s, float) else "",
                speedup=f"{speedup:.3g}" if speedup is not None else "",
                rust_rss=rust_rss or "",
                c_rss=c_rss or "",
                rss_ratio=f"{rss_ratio:.3g}" if rss_ratio is not None else "",
                size_ratio=f"{size_ratio:.3g}" if size_ratio is not None else "",
                status=status,
            )
        )
    failures = [
        row
        for row in rows
        if row["status"] != "ok" or row["verified"] in (False, "False", "false")
    ]
    if failures:
        summary_lines.extend(
            [
                "",
                "## Failures",
                "",
                "| dataset | impl | mode | frame | codec | filter | dict | typesize | threads | class | status | verified | stderr |",
                "|---|---|---|---|---|---|---:|---:|---:|---|---|---|---|",
            ]
        )
        for row in failures:
            stderr = str(row["stderr"]).replace("\n", " ")[:160]
            summary_lines.append(
                "| {dataset} | {impl} | {mode} | {frame} | {codec} | {filter} | {dict} | {typesize} | {threads} | {class_} | {status} | {verified} | {stderr} |".format(
                    dataset=row["dataset"],
                    impl=row["impl"],
                    mode=row["mode"],
                    frame=row["frame_impl"],
                    codec=row["codec"],
                    filter=row["filter"],
                    dict="yes" if row["use_dict"] else "no",
                    typesize=row["typesize"],
                    threads=row["nthreads"],
                    class_=row["failure_class"],
                    status=row["status"],
                    verified=row["verified"],
                    stderr=stderr,
                )
            )
    (output_dir / "summary.md").write_text("\n".join(summary_lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--manifest", type=Path, help="TOML manifest with [[dataset]] entries")
    source.add_argument("--fixtures-dir", type=Path, help="directory of fixture files, all as typesize 1")
    parser.add_argument("--profile", choices=["quick", "publish", "full"], default="quick")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--rust-bin", type=Path, help="prebuilt Rust blosc2 binary")
    parser.add_argument("--c-helper", type=Path, help="prebuilt c_blosc2_file_bench binary")
    parser.add_argument("--keep-work", action="store_true", help="keep per-case temporary files")
    args = parser.parse_args()

    datasets = load_manifest(args.manifest) if args.manifest else discover_datasets(args.fixtures_dir)
    for dataset in datasets:
        if not dataset.path.exists():
            raise SystemExit(f"dataset does not exist: {dataset.path}")

    rust_bin = build_rust(args)
    c_helper = build_c_helper(args)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_dir or (REPO / "target/real-bench/results" / stamp)

    rows: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(prefix="blosc2-real-bench-", dir=REPO / "target") as tmp:
        tmp_root = Path(tmp)
        for dataset in datasets:
            for idx, case in enumerate(cases_for_profile(args.profile, dataset.typesizes)):
                case_dir = tmp_root / dataset.name / str(idx)
                case_dir.mkdir(parents=True, exist_ok=True)
                print(
                    f"{dataset.name} {idx}: codec={case.codec} clevel={case.clevel} "
                    f"filter={case.filter} dict={case.use_dict} "
                    f"typesize={case.typesize} threads={case.nthreads}",
                    flush=True,
                )
                run_case(rust_bin, c_helper, dataset, case, case_dir, rows)
        if args.keep_work:
            kept = output_dir / "work"
            if kept.exists():
                shutil.rmtree(kept)
            kept.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(tmp_root, kept)

    if not rows:
        raise SystemExit("no benchmark rows produced")
    write_outputs(rows, output_dir)
    print(f"wrote {output_dir / 'results.csv'}")
    print(f"wrote {output_dir / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
