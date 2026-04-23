import csv
import io
from openpyxl import Workbook
from .models import Sheet


def _row_value(cell):
    if isinstance(cell, dict):
        return cell.get("value")
    return cell


def to_csv(sheet: Sheet) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(sheet.headers)
    for r in sheet.rows:
        w.writerow([_row_value((r.cells or {}).get(h)) for h in sheet.headers])
    return buf.getvalue()


def to_xlsx(sheet: Sheet) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = sheet.name[:31] or "Sheet"
    ws.append(sheet.headers)
    for r in sheet.rows:
        ws.append([_row_value((r.cells or {}).get(h)) for h in sheet.headers])
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()
