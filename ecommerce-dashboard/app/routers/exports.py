from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from starlette.background import BackgroundTask

from app.services.exports import (
    cleanup_temp_export,
    create_full_backup_archive,
    create_period_export_archive,
    restore_from_backup_archive,
)


router = APIRouter(prefix="/api/exports", tags=["exports"])


@router.get("/backup")
def api_export_backup() -> FileResponse:
    archive = create_full_backup_archive()
    return FileResponse(
        path=archive.file_path,
        media_type="application/zip",
        filename=archive.download_name,
        background=BackgroundTask(cleanup_temp_export, archive.file_path),
    )


@router.get("/period")
def api_export_period(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    marketplace: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
) -> FileResponse:
    try:
        archive = create_period_export_archive(
            from_date=from_date,
            to_date=to_date,
            marketplace=marketplace,
            query=q,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return FileResponse(
        path=archive.file_path,
        media_type="application/zip",
        filename=archive.download_name,
        background=BackgroundTask(cleanup_temp_export, archive.file_path),
    )


@router.post("/restore")
async def api_restore_backup(file: UploadFile) -> JSONResponse:
    """
    Restore from a full backup ZIP archive.
    Accepts multipart file upload. The ZIP must contain a valid
    manifest.json with kind="full_backup".
    """
    if file.filename and not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Nur ZIP-Dateien werden akzeptiert.")

    # Write uploaded file to a temp location
    temp_dir = Path(tempfile.mkdtemp(prefix="restore-upload-"))
    temp_zip_path = temp_dir / "upload.zip"

    try:
        with open(temp_zip_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                f.write(chunk)

        result = restore_from_backup_archive(temp_zip_path)

    finally:
        # Clean up the uploaded temp file
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass

    if not result.success:
        error_msg = result.summary.get("error", "Wiederherstellung fehlgeschlagen")
        return JSONResponse(
            status_code=400 if "error" in result.summary else 500,
            content={"success": False, "error": error_msg, "details": result.summary},
        )

    return JSONResponse(
        status_code=200,
        content={"success": True, "summary": result.summary},
    )
