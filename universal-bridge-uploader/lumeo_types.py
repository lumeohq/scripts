from typing import Union, Dict, List

JsonValue = Union[str, None, int, float, bool, "JsonObject", List["JsonValue"]]
JsonObject = Dict[str, JsonValue]