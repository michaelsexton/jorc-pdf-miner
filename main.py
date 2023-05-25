import hashlib

import os

import fitz
import img2table.ocr
from fitz import Pixmap
from img2table.document import Image

JORC_WORDS = ["PROVED", "PROBABLE", "MEASURED", "INDICATED", "INFERRED"]

ocr = img2table.ocr.TesseractOCR()


def get_file_hash(filename: str) -> str:
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def extract_images(file: str, image_path: str):
    pdf = fitz.open(file)
    for page_index in range(len(pdf)):
        page = pdf[page_index]
        image: Pixmap = page.get_pixmap(dpi=200)
        image.save(os.path.join(image_path, f'page_{page_index}.png'), "PNG")
        # image = page.get_pixmap().get_image_data(output='png')
        # img = Image.frombytes('RGB', (image.width, image.height), image.samples)
        # img.save(os.path.join(image_path,f'page_{page_index}.png'), 'PNG')


def read_pdfs():
    pdf_directory = "pdfs"
    image_directory = "images"
    files = [os.path.join(pdf_directory, f) for f in os.listdir(pdf_directory)]
    for file in files:
        hash_path = get_file_hash(file)
        image_path = os.path.join(image_directory, hash_path)
        os.makedirs(image_path, exist_ok=True)
        extract_images(file, image_path)
        extract_tables(image_path)


def extract_tables(image_path):
    # image_directory = "images"
    excel_directory = "output"
    os.makedirs(excel_directory, exist_ok=True)
    files = [os.path.join(image_path, f) for f in os.listdir(image_path)]
    # TODO make this a lambda
    for i, file in enumerate(files):
        img = Image(src=file)
        tables = img.extract_tables(ocr=ocr, implicit_rows=False,
                                    borderless_tables=False,
                                    min_confidence=50)
        if tables:
            output = os.path.join(excel_directory, "page-{0}.xlsx".format(i))

            tables[0].df.to_excel(output, index=False)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    read_pdfs()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/