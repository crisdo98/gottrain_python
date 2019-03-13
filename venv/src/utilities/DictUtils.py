class DictUtils:

    @staticmethod
    def get_value(key, data):
        keys = key.split(".")
        val = data
        for idx, k in enumerate(keys):
            if isinstance(val, list):
                return str(val)
            else:
                try:
                    val = val.get(k, "")
                except:
                    val = ""
                if val == "":
                    return val
            if isinstance(val, list):
                for i in val:
                    if isinstance(i, str):
                        val = ", ".join(val)
                        return val
            elif isinstance(val, str):
                return val.strip()
        return str(val)
