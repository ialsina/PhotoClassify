import os
import shutil
import hashlib
from logging import Logger

from .log import get_logger

LOGGER = get_logger(__name__)

def calculate_hash(path: str, logger: Logger = LOGGER) -> str:
    """Calculate MD5 hash of a file for integrity checking."""
    hash_md5 = hashlib.md5()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as exc:
        logger.error("Failed to calculate hash for %s: %s", path, exc)
        return None

def check_disk_space(path: str, required_size: int, logger: Logger = LOGGER) -> bool:
    """Check if the destination has enough space for the file."""
    try:
        stat = os.statvfs(path)
        available_space = stat.f_bavail * stat.f_frsize  # Available space in bytes
        return available_space >= required_size
    except Exception as exc:
        logger.error("Error checking disk space for %s: %s", path, exc)
        return False

def cp(src: str, dst: str, buffer_size: int = 1024*1024, logger: Logger = LOGGER) -> None:
    """Move a large file in chunks, ensuring atomic operation."""
    try:
        with open(src, "rb") as f_src, open(dst, "wb") as f_dst:
            while chunk := f_src.read(buffer_size):
                f_dst.write(chunk)
        shutil.copystat(src, dst)
        logger.info("Successfully moved large file %s to %s", src, dst)
    except Exception as exc:
        logger.error("Error during chunked move from %s to %s: %s", src, dst, exc)
        raise exc

def mv(src: str, dst: str, logger: Logger = LOGGER) -> None:
    """Safely move a file with space checking, atomic moves, and integrity verification."""
    if not os.path.exists(src):
        logger.error("Source file does not exist: %s", src)
        return
    
    # Ensure the destination directory exists
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    # Check disk space
    file_size = os.path.getsize(src)
    if not check_disk_space(os.path.dirname(dst), file_size):
        logger.error("Not enough space at destination %s", os.path.dirname(dst))
        return

    # Create a temporary file for atomic operation
    temp_dst = dst + ".tmp"
    try:
        # Move file in chunks
        cp(src, temp_dst)

        # Verify file integrity
        src_hash = calculate_hash(src)
        dst_hash = calculate_hash(temp_dst)

        if src_hash and dst_hash and src_hash == dst_hash:
            logger.info("Integrity check passed for file %s", src)
            os.rename(temp_dst, dst)  # Atomic rename
            os.remove(src)
            logger.info("File successfully moved from %s to %s", src, dst)
        else:
            logger.error("Integrity check failed for file %s", src)
            os.remove(temp_dst)  # Clean up the temporary file if integrity fails
    except Exception as exc:
        logger.error("Failed to move file %s to %s: %s", src, dst, exc)
        if os.path.exists(temp_dst):
            os.remove(temp_dst)  # Ensure temporary file is cleaned up

def rm(path: str, logger: Logger = LOGGER) -> None:
    """Remove a file and log the operation."""
    if os.path.exists(path):
        os.remove(path)
        logger.info("File successfully removed: %s", path)
    else:
        logger.error("File not found: %s", path)
