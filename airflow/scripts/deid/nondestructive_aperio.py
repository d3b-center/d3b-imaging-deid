"""
This module deidentifies Aperio SVS slide files NON-destructively.
Invoke deid_aperio_svs(in_filename, out_filename) and a new deidentified file
will be written.

This code should have pretty much the same result as the destructive variant
but uses a different internal mechanism.
"""

import tifftools
import sys
import re


class UnrecognizedFile(Exception):
    pass


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


def deid_aperio_svs(in_filename, out_filename):
    tiffinfo = tifftools.read_tiff(in_filename)
    input_ifds = tiffinfo["ifds"]

    msgs = f"{len(input_ifds)}"
    kept_ifds = []
    for i, ifd in enumerate(input_ifds):
        if tifftools.Tag.SubFileType.value not in ifd["tags"]:
            raise UnrecognizedFile(f"IFD {i} missing SUBFILETYPE")

        sft = ifd["tags"][tifftools.Tag.SubFileType.value]["data"][0]
        if sft == 0:
            # Only label and macro IFDs should have no description
            if tifftools.Tag.ImageDescription.value not in ifd["tags"]:
                raise UnrecognizedFile(f"IFD {i} missing IMAGE_DESCRIPTION")

            img_desc = ifd["tags"][tifftools.Tag.ImageDescription.value][
                "data"
            ].strip()

            if not img_desc.startswith("Aperio"):
                raise UnrecognizedFile(
                    f"Description {i} doesn't start with Aperio"
                )

            # Last line of description should start with image dimensions.
            # This may be purely anecdotal. Let's confirm.
            if not re.match("^\d+x\d+", img_desc.split("\n")[-1]):
                raise UnrecognizedFile(
                    f"IFD {i} IMAGE_DESCRIPTION last line doesn't start with dimensions"
                )

            first, *rest = img_desc.split("|")
            parts = [first]
            for r in rest:
                s = re.split(r"\s*=\s*", r, maxsplit=1)
                if s[0].lower() in KEEP:
                    parts.append(f"{s[0]} = {s[1]}")

            parts = "|".join(parts)
            if parts != img_desc:
                ifd["tags"][tifftools.Tag.ImageDescription.value] = {
                    "data": parts,
                    "datatype": tifftools.Datatype.ASCII,
                }
                msgs += "R"
            else:
                msgs += "K"

            kept_ifds.append(ifd)
        elif sft in LABEL_SFT:
            # https://github.com/openslide/openslide/issues/297#issuecomment-813424628
            msgs += "D"
        elif i < 2:
            raise UnrecognizedFile("Missing initial non-label IFDs")
        else:
            raise UnrecognizedFile(
                f"IFD {i} SUBFILETYPE {sft} isn't 0, 1, or 9"
            )

    tifftools.write_tiff(kept_ifds, out_filename, allowExisting=True)
    return msgs


if __name__ == "__main__":
    deid_aperio_svs(sys.argv[1], sys.argv[2])
