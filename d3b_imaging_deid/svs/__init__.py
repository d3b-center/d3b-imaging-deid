"""
This module deidentifies Aperio SVS slide files.
Invoke deid_aperio_svs(filepath) and the file will be destructively deidentified in-place.
"""

import re

from d3b_imaging_deid.svs.tiff import IMAGE_DESCRIPTION, TiffFile, UnrecognizedFile

TIFF_SUBFILETYPE = 254

KEEP = {
    "appmag",
    "displaycolor",
    "exposure scale",
    "exposure time",
    "focus offset",
    "filtered",
    "gamma",
    "icc profile",
    "left",
    "lineareaxoffset",
    "lineareayoffset",
    "linecameraskew",
    "mpp",
    "originalheight",
    "originalwidth",
    "stripewidth",
    "top",
}

LABEL_SFT = {1: "label", 9: "macro"}


def confirm_aperio_svs(tiff_handle):
    """Confirm that an open TiffFile handle points to an Aperio SVS. If this
    function returns without raising an exception, the file is confirmed.

    :param tiff_handle: an open TIFF handle
    :type tiff_handle: TiffFile
    :raises UnrecognizedFile: if any confirmation checks fail
    """
    # desc0 = tiff_handle.directories[0].entries[IMAGE_DESCRIPTION].value()
    # if not desc0.startswith(b"Aperio"):
    #     raise UnrecognizedFile("Image Description doesn't start with Aperio")
    for i in reversed(range(len(tiff_handle.directories))):
        dir = tiff_handle.directories[i]
        if TIFF_SUBFILETYPE not in dir.entries:
            raise UnrecognizedFile(f"IFD {i} missing SUBFILETYPE")

        sft = dir.entries[TIFF_SUBFILETYPE].value()[0]
        if sft == 0:
            if IMAGE_DESCRIPTION not in dir.entries:
                raise UnrecognizedFile(f"IFD {i} missing IMAGE_DESCRIPTION")

            img_desc = dir.entries[IMAGE_DESCRIPTION].value().decode().strip()

            if not img_desc.startswith("Aperio"):
                raise UnrecognizedFile(f"Description {i} doesn't start with Aperio")

            if not re.match("^\d+x\d+", img_desc.split("\n")[-1]):
                # The last line of the description should start with dimensions.
                # This may be purely anecdotal. Let's confirm.
                raise UnrecognizedFile(
                    f"IFD {i} IMAGE_DESCRIPTION last line doesn't start with dimensions"
                )
        elif sft not in LABEL_SFT:
            raise UnrecognizedFile(f"IFD {i} SUBFILETYPE {sft} isn't 0, 1, or 9")
        elif i < 2:
            raise UnrecognizedFile("Missing initial non-label IFDs")


def deid_aperio_svs(filepath):
    """Remove label and macro IFDs from an Aperio SVS TIFF file and redact
    any non-image-metric values from all Image Description fields.

    WARNING: THIS IS DESTRUCTIVE

    :param filepath: path to an Aperio SVS file
    :type filepath: str
    """
    msgs = []
    with TiffFile(filepath) as tf:
        confirm_aperio_svs(tf)
        for dir in reversed(tf.directories):
            sft = dir.entries[TIFF_SUBFILETYPE].value()[0]
            if sft == 0:
                # Remove everything but image metrics from the ImageDescriptions
                img_desc = dir.entries[IMAGE_DESCRIPTION].value().decode().strip()
                first, *rest = img_desc.split("|")
                parts = [first]

                for r in rest:
                    s = re.split(r"\s*=\s*", r, maxsplit=1)
                    if s[0].lower() in KEEP:
                        parts.append(f"{s[0]} = {s[1]}")

                parts = "|".join(parts)
                if parts != img_desc:
                    dir.entries[IMAGE_DESCRIPTION].overwrite_entry(parts.encode())
                    msgs.append("R")
                else:
                    msgs.append("K")
            elif sft in LABEL_SFT:
                # Delete label and macro IFDs
                # https://github.com/openslide/openslide/issues/297#issuecomment-813424628
                dir.delete()
                msgs.append(f"D{LABEL_SFT[sft]}")

        return f"{len(tf.directories)} {','.join(reversed(msgs))}"
