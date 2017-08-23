import jsonlines

from annotations.loader import AnnotationsLoader

def main():
    with AnnotationsLoader() as loader:
        with jsonlines.open("/home/godfoder/Downloads/annotations.jsonl", mode="r") as jf:
            def tuplify(al):
                count = 0
                for a in al:
                    count += 1
                    yield (a, True)

                    if count % 10000 == 0:
                        print(count)

            loader.add_corrections_iter(tuplify(jf))


if __name__ == '__main__':
    main()
