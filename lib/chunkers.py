import itertools as it


def simple_idchunker(iterable, chunk_size=1000):
    """Evenly chunked indices with generator

    Creates a generator that returns index values mapped to iterable. Each call
    returns a list of unique index values of chunk size or less

    The generator is effectively an enumerator, just returns index values in
    chunk size (or less) instead of one at a time

    Args:
        iterable (iterable):
            An object that is iterable, such as iterators, generators, etc.

        chunk_size (int):
            The size of each returned collection

    Returns (list(int)):
        The generator returns a list of index values of chunk size or less.
        Each call returns the next set of unique index values
    """
    current_index = 0
    result_indices = list()

    for _ in iterable:
        result_indices.append(current_index)
        current_index += 1

        if len(result_indices) == chunk_size:
            yield result_indices

            result_indices = list()

    if len(result_indices) != 0:
        yield result_indices


def get_nrows(file, read_mode="rb"):
    counter = 0
    with open(file, read_mode) as f:
        for a_line in f:
            counter += 1

    return counter


class Rowread(object):
    """Text file reader that seeks line number

    A Rowread class is effectively a text file reader. The key difference is
    that the class can seek for specific lines (row) to start reading from, as
    well as read n number of lines from that point

    Objects of this class keeps the file open. It is strongly recommended that
    objects of this class is used with the with statement to ensure the file
    is closed appropriately. The with statement is not yet implemented however

    TODO implement with statement context manager
    """

    def __init__(self, file, start_row, read_mode="rb"):
        """ Rowread constructor

        Create an object of class Rowread, opening a file with initial start
        row for reading

        Args
        ------
        file : str
            Path to directory and filename to read from

        start_row : int
            Line number to start reading from. Line numbers are indexed
            starting from 1

        read_mode : str
            The file mode to open the file in. This is the same as open() in
            Python. This value is pass directly to it

        Returns
        ---------
          A Rowread object that is connected to the provided file. The file is
          not initially open. To read, you must use the with statement
        """
        self.filename = file
        self.file = None
        self.start_row = start_row
        self.current_row = 1
        self.read_mode = read_mode

    def __enter__(self):
        self.file = open(self.filename, self.read_mode)

        return self

    def __exit__(self, *args):
        self.__close__()

    def __close__(self):
        if self.file is not None:
            self.file.close()
            self.file = None

    def __open__(self):
        if self.file is None:
            self.file = open(self.filename, self.read_mode)

    def reset(self, start_row):
        """ Reset reading to a new start row/line number

        This method effectively close and re-opens the file, setting the start
        row to the user provided parameter

        Args
        ------
        start_row : int
            Line number to start reading from. Line numbers are indexed
            starting from 1
        """
        if self.file is None:
            raise Exception("File not open. Use with statement")

        self.__close__()
        self.__open__()
        self.current_row = 1

        self.set_startrow(start_row)

    def set_startrow(self, start_row):
        """ Sets the file reading start row

        User can set the start row before reading. However, if the current row
        is greater than the provided start row, this method will raise an
        Exception

        Essentially, you cannot start at a row the reader has already past
        e.g. The file is current at row 1000, the user cannot go to rows < 1000

        If you do want to go backwards, you must use the reset(...) method to
        set a new start row

        Args
        ------
        start_row : int
            Line number to start reading from. Line numbers are indexed
            starting from 1
        """
        if start_row < self.current_row:
            raise Exception("Start row is lower than current row")

        else:
            self.start_row = start_row

    def read(self, nrows):
        """ Retrieve data from text file

        Start from the start row, read nrows from the text file

        Args
        ------
        nrows : int
            Number of rows to read from text file starting from start row

        Return : list(str)
        --------
        Each string corresponds to one row in the text file
        """
        if self.file is None:
            raise Exception("Text file is not open. Please use with statement")

        while self.current_row < self.start_row:
            next(self.file)
            self.current_row += 1

        result = list()
        rrows = 0
        for a_row in self.file:
            result.append(a_row)
            self.current_row += 1
            rrows += 1

            if rrows >= nrows:
                break

        if len(result) == 0:
            result = None

        return result



def row_reader(file, start_n, nrows, read_mode="rb"):
    counter = 0
    rows_read = 0

    with open(file, read_mode) as f:
        for a_line in f:
            counter += 1

            if counter >= start_n:
                rows_read += 1

                if rows_read <= nrows:
                    yield a_line

                else:
                    break

            else:
                pass


def simple_chunker(iterable, chunk_size=1000):
    """Evenly chunked iterator with generator

    Creates a generator that returns elements within the supplied iterator of
    approximately equal size. This avoids loading the whole iterator into
    memory (unless the iterable already exists in memory such as lists)

    Args:
        iterable (iterable):
            An object that is iterable, such as iterators, generators, etc.

        chunk_size (int):
            The size of each returned collection

    Returns (list(iterable)):
        The generator returns a list of elements retrieved from iterable of
        length chunk_size or less
    """
    size_counter = 0
    current_index = 0
    result_yield = list()
    for an_element in iterable:
        result_element = {"chunk": an_element, "id": current_index}
        size_counter += 1
        current_index += 1

        result_yield.append(result_element)

        if size_counter == chunk_size:
            yield result_yield

            result_yield = list()
            size_counter = 0

    if len(result_yield) != 0:
        yield result_yield


def sliding_chunker(iterable, chunk_size=1000, inc=2):
    """Sliding window chunked iterator with generator

    Creates a generator that returns elements within the supplied iterator of
    of length chunk_size. Each subsequent call will shift the window forward by
    inc while maintaining length chunk_size

    Args:
        iterable (iterable):
            An object that is iterable, such as iterators, generators, etc.

        chunk_size (int):
            The size of each returned collection

        inc (int):
            Sliding window increment size

    Returns (list(iterable)):
        The generator returns a list of elements retrieved from iterable of
        length chunk_size or less, shifted by inc forward (except first call)
    """
    chunking_iter = iter(iterable)
    element_block = list(it.islice(chunking_iter, chunk_size))

    if len(element_block) == chunk_size:
        yield element_block

    counting_index = 0
    inc_elements = list()
    for an_element in chunking_iter:
        inc_elements += [an_element]
        counting_index += 1

        if (counting_index % inc) is 0:
            element_block = element_block[inc:] + inc_elements

            yield element_block

            counting_index = 0
            inc_elements = list()
