import magic

def default_format_validator(url, t, fmt, content):
    mime = magic.from_buffer(content, mime=True)
    return (fmt == mime, mime)

def audio_mpeg3_validator(url, t, fmt, content):
    _, mime = default_format_validator(url, t, fmt, content)
    return (mime in ["audo/mpeg3", "audio/mpeg", "audio/mp3"], mime)

format_validators = {
    "model/mesh": lambda url, t, fmt, content: (url.endswith(".stl"), "model/mesh"),
    "audio/mpeg3": audio_mpeg3_validator
}

def get_validator(m):
    if m in format_validators:
        return format_validators[m]
    else:
        return default_format_validator