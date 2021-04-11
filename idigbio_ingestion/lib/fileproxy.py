"""
    Contains a class for proxying file objects for the purpose of monitoring the character stream and logging errors with better context information

    For example, the following code will read a csv line by line, and if there are any errors, will be able to log the context around which that error occured.

    import unicodecsv as csv
    import traceback

    with FileProxy(open("test.csv", "rb")) as inf:
        try:
            cr = csv.reader(inf)
            for l in cr:
                pass
                inf.snap()
        except:
            traceback.print_exc()
            x = inf.dump()
            print len(x), x

"""

class FileProxy(object):
    """
       The main FileProxy class, behaves just like a file object, with three additions:
         * dump() is added to return a string context of all bytes read since the last snapshot.
         * snap() is added to take a snapshot of the number of bytes read so far.
         * next() and read() are modified to capture the number of bytes read from the file.
    """
    def __init__(self, file, add=0):
        self.__file = file
        self.__add = add
        self.__read_chars = 0
        self.snap()

    def snap(self):
        self.__snapshot = self.__read_chars

    def cur_snap(self):
        return self.__snapshot

    def dump(self):
        save = self.__file.tell()
        self.__file.seek(max(self.__snapshot - self.__add, 0))
        s = self.__file.read(self.__read_chars - self.__snapshot + (self.__add * 2))
        self.__file.seek(save)
        return s

    def close(self):
        return self.__file.close()

    def flush(self):
        return self.__file.flush()

    def fileno(self):
        return self.__file.fileno()

    def isatty(self):
        return self.__file.isatty()

    def next(self):
        x = self.__file.next()
        self.__read_chars += len(x)
        return x

    def read(self,size=None):
        x = self.__file.read(size)
        self.__read_chars += len(x)
        return x

    def readline(self,size=None):
        return self.__file.readline(size)

    def readlines(self,sizehint=None):
        return self.__file.readlines(sizehint)

    def seek(self,offset,whence=None):
        return self.__file.seek(offset,whence)

    def tell(self):
        return self.__file.tell()

    def truncate(self,size=None):
        return self.__file.truncate(size)

    def write(self,str):
        return self.__file.write(str)

    def writelines(self,sequence):
        return self.__file.writelines(sequence)

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.__file.__exit__(exc_type, exc_val, exc_tb)

    closed = property(lambda me : getattr(me.__file, 'closed'))
    encoding = property(lambda me : getattr(me.__file, 'encoding'))
    errors = property(lambda me : getattr(me.__file, 'errors'), lambda me, val : setattr(me.__file, 'errors', val))
    mode = property(lambda me : getattr(me.__file, 'mode'))
    newlines = property(lambda me : getattr(me.__file, 'newlines'))
    softspace = property(lambda me : getattr(me.__file, 'softspace'),lambda me, val : setattr(me.__file, 'softspace', val))

def main():
    import unicodecsv as csv
    import traceback

    with FileProxy(open("test.csv", "rb")) as inf:
        try:
            cr = csv.reader(inf)
            for _ in cr:
                pass
                inf.snap()
        except:
            traceback.print_exc()
            x = inf.dump()
            print (len(x), x)

if __name__ == "__main__":
    main()
