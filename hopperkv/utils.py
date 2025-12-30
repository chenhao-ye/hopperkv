def str_cast_type(s, type_cast, binary_scale: bool):
    degree = 1024 if binary_scale else 1000
    if s.endswith("G") or s.endswith("g"):
        return type_cast(s[:-1]) * degree * degree * degree
    if s.endswith("M") or s.endswith("m"):
        return type_cast(s[:-1]) * degree * degree
    if s.endswith("K") or s.endswith("k"):
        return type_cast(s[:-1]) * degree
    if s.endswith("Gi"):
        assert binary_scale
        return type_cast(s[:-2]) * degree * degree * degree
    if s.endswith("Mi"):
        assert binary_scale
        return type_cast(s[:-2]) * degree * degree
    if s.endswith("Ki"):
        assert binary_scale
        return type_cast(s[:-2]) * degree
    return type_cast(s)
