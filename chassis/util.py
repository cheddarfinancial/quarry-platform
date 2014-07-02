# Standard Library
import time
import uuid

# Third Party

# Local


def makeHandle():
    """
    Generates a unique query handle. Not completely resistant to
    collisions but it's nearly impossible so... close enough

    Returns:
        a unique handle
    """

    return "%s_%s" % (
        uuid.uuid4().bytes.encode("base64")[:21].translate(None, "/+"),
        int(time.time()*1000)
    )

def processFormattedTableDescription(rows):

    section = ""
    cols = []
    cached = False
    for row in rows:

        if len(row[0]) > 0 and row[0][0] == "#":

            section = row[0].strip()

        elif section == "# col_name" and len(row[0]) > 0:

            cols.append({
                "name": row[0].strip(),
                "type": row[1].strip(),
                "comment": row[2].strip()
            })

        elif section == "# Detailed Table Information":

            if row[1].strip() == "shark.cache" and row[2].strip() != "NONE":
                cached = True

    return cols, cached
def processFormattedTableDescriptionHQL(rows):

    section = ""
    cols = []
    cached = False
    for row in rows:

        if len(row['_c0']) > 0 and row['_c0'][0] == "#":

            section = row['_c0'].strip()

        elif section == "# col_name" and len(row['_c0']) > 0:

            cols.append({
                "name": row['_c0'].strip(),
                "type": row['_c1'].strip(),
                "comment": row['_c2'].strip()
            })

        elif section == "# Detailed Table Information":

            if row['_c1'].strip() == "shark.cache" and row['_c2'].strip() != "NONE":
                cached = True

    return cols, cached
