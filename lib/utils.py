import itertools as it


def simple_idchunker(iterable, chunk_size=1000):
    """Evenly chunked indices with generator

    Creates a generator that returns index values. Each call returns a list of
    unique index values of chunk size or less

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
    result_yield = list()
    for an_element in iterable:
        result_yield.append(an_element)
        size_counter += 1

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
