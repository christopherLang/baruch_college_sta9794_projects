import os
import bz2
import json


def json_read(rootdir, filename_only=True):
    for rdir, sdir, filename in os.walk(rootdir):
        for afile in filename:
            if (afile.endswith(".json.bz2") and
                    afile.startswith(".") is not True):
                filepath = os.path.join(rdir, afile)
                filepath = filepath.replace("\\", "/").encode("utf-8")

                if filename_only is False:
                    with bz2.BZ2File(filepath, "rb") as f:
                        try:
                            tw_data = f.readlines()
                            tw_data = [i.decode("utf-8") for i in tw_data]
                            tw_data = [json.loads(i) for i in tw_data]

                        except Exception as e:
                            pass

                else:
                    tw_data = filepath.decode("utf-8")

                yield tw_data
