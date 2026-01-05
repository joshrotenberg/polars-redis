"""MkDocs hooks for copying static files."""

import shutil
from pathlib import Path


def on_post_build(config, **kwargs):
    """Copy slides directory to the built site."""
    docs_dir = Path(config["docs_dir"])
    site_dir = Path(config["site_dir"])

    # Copy slides directory
    slides_src = docs_dir / "slides"
    slides_dst = site_dir / "slides"

    if slides_src.exists():
        if slides_dst.exists():
            shutil.rmtree(slides_dst)
        shutil.copytree(slides_src, slides_dst)
