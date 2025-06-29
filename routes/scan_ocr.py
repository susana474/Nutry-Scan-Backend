from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List
import uuid
import asyncio
import time

from services.ocr_service import analizar_ingredientes_por_imagen
from services.supabase_upload import subir_imagen
from db.db_session import get_db
from models.scan_model import Escaneo
from datetime import datetime

from routes import scan_ocr
app.include_router(scan_ocr.router)


router = APIRouter()


async def subir_imagen_async(nombre_archivo: str, contenido: bytes):
    """Wrapper asíncrono para subir imagen"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, subir_imagen, nombre_archivo, contenido)


@router.post("/scan/ingredientes")
async def escanear_ingredientes(
        usuario_id: int,
        files: List[UploadFile] = File(...),
        db: Session = Depends(get_db)
):
    start_time = time.time()

    if not files:
        raise HTTPException(status_code=400, detail="Debes subir al menos una imagen")

    print(f"⏱️ Iniciando procesamiento de {len(files)} imagen(es)...")

    # 1. Leer contenido de imágenes (RÁPIDO)
    files_data = []
    for file in files[:2]:
        contenido = await file.read()
        files_data.append({
            'contenido': contenido,
            'filename': file.filename,
            'content_type': file.content_type
        })

    read_time = time.time()
    print(f"📖 Lectura completada en {read_time - start_time:.2f}s")

    # 2. PARALELIZAR: Subir imágenes Y analizar con Gemini simultáneamente
    async def subir_todas_imagenes():
        """Sube todas las imágenes en paralelo"""
        tasks = []
        for file_data in files_data:
            nombre = f"{usuario_id}_{uuid.uuid4().hex}_{file_data['filename']}"
            task = subir_imagen_async(nombre, file_data['contenido'])
            tasks.append(task)

        try:
            return await asyncio.gather(*tasks)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error subiendo imágenes: {e}")

    # Ejecutar subida de imágenes y análisis en paralelo
    print("🚀 Ejecutando subida y análisis en paralelo...")

    try:
        # Ambas operaciones en paralelo
        upload_task = subir_todas_imagenes()
        analysis_task = analizar_ingredientes_por_imagen(files_data)

        urls, resultado = await asyncio.gather(upload_task, analysis_task)

    except Exception as e:
        if "subiendo" in str(e):
            raise e
        else:
            raise HTTPException(status_code=500, detail=f"Error en análisis: {e}")

    parallel_time = time.time()
    print(f"⚡ Operaciones paralelas completadas en {parallel_time - read_time:.2f}s")

    # 3. Guardar en base de datos (RÁPIDO)
    escaneo = Escaneo(
        usuario_id=usuario_id,
        datos=resultado,
        imagenes=urls,
        fecha=datetime.utcnow()
    )

    db.add(escaneo)
    db.commit()
    db.refresh(escaneo)

    total_time = time.time()
    print(f"💾 Guardado completado en {total_time - parallel_time:.2f}s")
    print(f"🎯 TIEMPO TOTAL: {total_time - start_time:.2f}s")

    return {
        "msg": "✅ Escaneo registrado",
        "id": escaneo.id,
        "resultado": resultado,
        "imagenes": urls,
        "tiempo_procesamiento": f"{total_time - start_time:.2f}s"
    }


@router.post("/scan/ingredientes-async")
async def escanear_ingredientes_background(
        background_tasks: BackgroundTasks,
        usuario_id: int,
        files: List[UploadFile] = File(...),
        db: Session = Depends(get_db)
):
    """
    Versión que procesa en background - retorna inmediatamente con ID
    El usuario puede consultar el estado después
    """
    if not files:
        raise HTTPException(status_code=400, detail="Debes subir al menos una imagen")

    # Crear registro inicial
    escaneo = Escaneo(
        usuario_id=usuario_id,
        datos={"status": "processing"},
        imagenes=[],
        fecha=datetime.utcnow()
    )

    db.add(escaneo)
    db.commit()
    db.refresh(escaneo)

    # Leer archivos ahora para no perder el stream
    files_data = []
    for file in files[:2]:
        contenido = await file.read()
        files_data.append({
            'contenido': contenido,
            'filename': file.filename,
            'content_type': file.content_type
        })

    # Agregar tarea en background
    background_tasks.add_task(
        procesar_en_background,
        escaneo.id,
        usuario_id,
        files_data
    )

    return {
        "msg": "✅ Procesamiento iniciado",
        "id": escaneo.id,
        "status": "processing"
    }


async def procesar_en_background(escaneo_id: int, usuario_id: int, files_data: list):
    """Procesa el escaneo en background"""
    from db.db_session import SessionLocal

    db = SessionLocal()
    try:
        # Operaciones en paralelo
        async def subir_todas():
            tasks = []
            for file_data in files_data:
                nombre = f"{usuario_id}_{uuid.uuid4().hex}_{file_data['filename']}"
                task = subir_imagen_async(nombre, file_data['contenido'])
                tasks.append(task)
            return await asyncio.gather(*tasks)

        urls, resultado = await asyncio.gather(
            subir_todas(),
            analizar_ingredientes_por_imagen(files_data)
        )

        # Actualizar registro
        escaneo = db.query(Escaneo).filter(Escaneo.id == escaneo_id).first()
        if escaneo:
            escaneo.datos = resultado
            escaneo.imagenes = urls
            db.commit()

        print(f"✅ Escaneo {escaneo_id} completado en background")

    except Exception as e:
        print(f"❌ Error en background para escaneo {escaneo_id}: {e}")
        # Actualizar con error
        escaneo = db.query(Escaneo).filter(Escaneo.id == escaneo_id).first()
        if escaneo:
            escaneo.datos = {"status": "error", "error": str(e)}
            db.commit()
    finally:
        db.close()


@router.get("/scan/status/{escaneo_id}")
async def obtener_estado_escaneo(escaneo_id: int, db: Session = Depends(get_db)):
    """Consulta el estado de un escaneo en background"""
    escaneo = db.query(Escaneo).filter(Escaneo.id == escaneo_id).first()

    if not escaneo:
        raise HTTPException(status_code=404, detail="Escaneo no encontrado")

    # Determinar estado
    if isinstance(escaneo.datos, dict) and escaneo.datos.get("status") == "processing":
        status = "processing"
    elif isinstance(escaneo.datos, dict) and escaneo.datos.get("status") == "error":
        status = "error"
    else:
        status = "completed"

    return {
        "id": escaneo.id,
        "status": status,
        "datos": escaneo.datos,
        "imagenes": escaneo.imagenes,
        "fecha": escaneo.fecha
    }
