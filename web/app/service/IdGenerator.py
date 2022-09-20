from sonyflake import SonyFlake

idGenerator = None

def getIdGenerator() -> SonyFlake:
    global idGenerator

    if idGenerator:
        return idGenerator

    idGenerator = SonyFlake()
    return idGenerator