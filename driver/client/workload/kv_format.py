"""Encode offset and size into key and value string"""

import random
import string
from dataclasses import dataclass


@dataclass
class KvFormatParams:
    key_size: int
    val_size: int
    size_len: int
    offset_len: int
    k_pad_len: int
    v_pad_len: int


def gen_rand_str(size):
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(size))


def make_key(offset: int, format_params: KvFormatParams) -> str:
    k = (
        f"K{offset:0{format_params.offset_len}}"
        f"s{format_params.key_size:0{format_params.size_len}}"
        + "E" * format_params.k_pad_len
        + "Y"
    )
    assert len(k) == format_params.key_size
    return k


def make_val(
    offset: int,
    format_params: KvFormatParams,
    use_rand: bool = False,
) -> str:
    v = (
        f"V{offset:0{format_params.offset_len}}"
        f"s{format_params.val_size:0{format_params.size_len}}"
        + "A" * format_params.v_pad_len
        + "L"
        if not use_rand
        else (
            f"V{offset:0{format_params.offset_len}}"
            f"s{format_params.val_size:0{format_params.size_len}}"
            + gen_rand_str(format_params.v_pad_len)
            + "L"
        )
    )
    assert len(v) == format_params.val_size
    return v


# compute size_len, offset_len, k_pad_len, v_pad_len
def get_format_params(key_size: int, val_size: int) -> KvFormatParams:
    size_len: int = max(len(str(key_size)), len(str(val_size)))
    least_len_left = min(key_size, val_size) - 3 - size_len
    assert least_len_left > 0

    # uint32_t can have 10 digitals, so if least_len < 10, no padding
    offset_len = least_len_left if least_len_left < 10 else 10

    k_pad_len = key_size - 3 - size_len - offset_len
    v_pad_len = val_size - 3 - size_len - offset_len
    assert k_pad_len >= 0
    assert v_pad_len >= 0

    # if offset is too larger than cannot be fitted into offset_len,
    # `make_key/val` will report assertion fail
    return KvFormatParams(
        key_size, val_size, size_len, offset_len, k_pad_len, v_pad_len
    )


# perform a quick check on the given; can have false positive (i.e., return
# True for unmatched string)
def check_quick(expected_val: str, actual_val: str, use_rand: bool = False) -> bool:
    if len(expected_val) != len(actual_val):
        return False
    if use_rand:  # if rand_val is enabled, only check the value length
        return True
    if len(expected_val) <= 32:
        return expected_val == actual_val
    return expected_val[:32] == actual_val[:32]


if __name__ == "__main__":
    # some tests
    key_size, val_size = 16, 40
    format_params = get_format_params(key_size, val_size)
    print(make_key(134, format_params))
    print(make_val(134, format_params))

    key_size, val_size = 8, 40
    format_params = get_format_params(key_size, val_size)
    print(make_key(345, format_params))
    print(make_val(345, format_params))

    # the two line below should fail
    # print(make_key(1345, format_params))
    # print(make_val(1345, format_params))

    key_size, val_size = 10, 20
    format_params = get_format_params(key_size, val_size)
    print(make_key(5345, format_params))
    print(make_val(5345, format_params))

    key_size, val_size = 40, 20
    format_params = get_format_params(key_size, val_size)
    print(make_key(2567, format_params))
    print(make_val(2567, format_params))

    key_size, val_size = 16, 500
    format_params = get_format_params(key_size, val_size)
    print(make_key(48_000_000, format_params))
    print(make_val(48_000_000, format_params))
