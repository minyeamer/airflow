import datetime as dt

def print_now():
    print(dt.datetime.now())


def regist(name: str, age: int, *args, **kwargs):
    print(f"이름: {name}")
    print(f"나이: {age}")
    for __key, __value in kwargs.items():
        print(f"{__key}: {__value}")
    if args:
        print(f"기타 정보: {args}")
