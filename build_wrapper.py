import shutil
import sys
from maturin import *  # Forward everything from maturin


def check_build_environment():
    # ANSI Escape Codes
    RED_BG = "\033[41m\033[1;37m"  # Bold white text on red background
    BOLD_RED = "\033[1;31m"  # Bold red text
    BOLD_GREEN = "\033[1;32m"  # Bold green text
    BOLD = "\033[1m"  # Bold text
    RESET = "\033[0m"  # Reset formatting

    has_compiler = shutil.which("cc") or shutil.which("gcc") or shutil.which("clang")
    has_cargo = shutil.which("cargo")

    # 1. Check for C Compiler / Linker
    if not has_compiler:
        print(
            f"\n{RED_BG}  BUILD ERROR: MISSING C COMPILER  {RESET}\n", file=sys.stderr
        )
        print(
            f"{BOLD_RED}A system C compiler / linker ('cc', 'gcc', or 'clang') was not found.{RESET}",
            file=sys.stderr,
        )
        print(
            f"{BOLD}This project compiles native Rust extensions and requires a system toolchain.{RESET}\n",
            file=sys.stderr,
        )

        print(
            "Please install a compiler using your system package manager:",
            file=sys.stderr,
        )
        print(
            f"  {BOLD_GREEN}Ubuntu/Debian{RESET}:  sudo apt update && sudo apt install build-essential",
            file=sys.stderr,
        )
        print(
            f'  {BOLD_GREEN}Fedora/RHEL{RESET}:    sudo dnf groupinstall "Development Tools"',
            file=sys.stderr,
        )
        print(
            f"  {BOLD_GREEN}Arch Linux{RESET}:     sudo pacman -S base-devel",
            file=sys.stderr,
        )
        print(
            f"  {BOLD_GREEN}macOS{RESET}:          xcode-select --install\n",
            file=sys.stderr,
        )
        sys.exit(1)

    # 2. Check for Cargo / Rust
    if not has_cargo:
        print(f"\n{RED_BG}  BUILD ERROR: MISSING RUST  {RESET}\n", file=sys.stderr)
        print(
            f"{BOLD_RED}The Rust compiler and package manager ('cargo') was not found.{RESET}",
            file=sys.stderr,
        )
        print(
            f"{BOLD}This project includes Rust code and requires the Rust toolchain to build.{RESET}\n",
            file=sys.stderr,
        )

        print("Please install Rust using the recommended installer:", file=sys.stderr)
        print(
            f"  {BOLD_GREEN}Linux & macOS{RESET}:  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh",
            file=sys.stderr,
        )
        print(
            f"  {BOLD_GREEN}Windows{RESET}:        Download and run rustup-init.exe from https://rustup.rs\n",
            file=sys.stderr,
        )
        sys.exit(1)


# Intercept and wrap maturin's PEP 517 entry hooks
def build_wheel(*args, **kwargs):
    check_build_environment()
    import maturin

    return maturin.build_wheel(*args, **kwargs)


def build_editable(*args, **kwargs):
    check_build_environment()
    import maturin

    return maturin.build_editable(*args, **kwargs)


def build_sdist(*args, **kwargs):
    import maturin

    return maturin.build_sdist(*args, **kwargs)
