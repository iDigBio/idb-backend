import json

from annotations.loader import AnnotationsLoader

def main():
    with AnnotationsLoader() as loader:
        with open("/home/godfoder/Downloads/annotations.json", "rb") as jf:
            annotation_list = json.load(jf)

            def tuplify(al):
                for a in al:
                    yield (a, True)

            loader.add_corrections_iter(tuplify(annotation_list))


if __name__ == '__main__':
    main()
