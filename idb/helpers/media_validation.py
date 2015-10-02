import magic

format_validators = {
    "model/mesh": lambda url, t, fmt, content: (url.endswith(".stl"), "model/mesh")
}

def default_format_validator(url, t, fmt, content):
    mime = magic.from_buffer(content, mime=True)
    return (fmt == mime, mime)

def get_validator(m):
    if m in format_validators:
        return format_validators[m]
    else:
        return default_format_validator