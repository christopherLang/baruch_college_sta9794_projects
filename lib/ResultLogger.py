import platform as plt
import textwrap
import datetime as dt
import os


class ResultLogger(object):
    def __init__(self, file, title, description=None, max_width=79, ind="  ",
                 left_margin=" ", right_margin=" ", default_header=True):
        self.file = file
        self.nlines = 0

        self.hborder_char = "="
        self.vborder_char = "|"
        self.max_width = max_width
        self.ind = ind
        self.left_margin = left_margin
        self.right_margin = right_margin

        # Open the file for writing
        if os.path.exists(file) is not True:
            log_file = open(file, "w")
            log_file.close()

        if default_header is True:
            dheaders = self.get_default_headers(title, description)

            self.__write_lines(dheaders, write_mode="w")

    def add_line(self, textline, level=0):
        """Add line item to results log

        A text line (textline) is a line item added after headers are placed

        Args:
            textline (str):
                A string that is immediately added to the results log file

            level (int):
                The level of indent to add

        Returns (bool):
            Nothing is returned. Line item is written to file
        """
        if level == 0:
            ind = ""

        else:
            ind = "".join([self.ind for _ in range(level)])

        textline = self.wrap_text(textline, self.left_margin + ind,
                                  self.right_margin, self.max_width)

        self.__write_lines(textline)

        self.nlines += 1

    def add_lines(self, textlines, level=0):
        """Add line items to results log

        A text line (textlines) is a line item added after headers are placed.
        This method allows for writing multiple lines at a time

        Args:
            textlines (list(str)):
                A list of strings that is immediately added to the results
                log file. Most data structures that are iterable would work

            level (int):
                The level of indent to add

        Returns (bool):
            Nothing is returned. Line items is written to file
        """
        if level == 0:
            ind = ""

        else:
            ind = "".join([self.ind for _ in range(level)])

        result = list()
        for a_line in textlines:
            result.extend(self.wrap_text(a_line, self.left_margin + ind,
                                         self.right_margin, self.max_width))

        self.__write_lines(textlines)

        self.nlines += len(textlines)

    def add_kvlines(self, key_values, level=0, k_justify="left", split=" : "):
        """Add a collection of key/values

        Key value is a common data point to add. kvlines ensures that the split
        lines up vertically, hence it is recommended to add several that are
        related in some way

        Args:
            key_values (list(tuple(str))):
                A list of tuples, where each tuples are key/value data

            level (int):
                The level of indent to add

            k_justify (str):
                Justification for the keys. Only left and right are allowed

            split (str):
                The string to use to delineate a key and a value when written
                to a line

        Returns (bool):
            Nothing is returned. Line items is written to file
        """
        lines = self.kv_format(key_values, level=level, k_justify=k_justify,
                               split=split)
        self.__write_lines(lines)

        self.nlines += len(lines)

    def get_default_headers(self, title, description=None):
        """Add a default header to the results file

        A default header contains program title, description and other items:
        1. Python information
        2. Host information
        3. TODO add performance data, such as CPU cores and RAM

        Args:
            title (str):
                The title of the program

            description (str):
                A brief, one paragraph (or more) description of the program

        Returns (list(str)):
            A list of strings containing the above information
        """
        pad_left = "| "
        pad_right = " |"

        h_border = self.__get_hborder(pad_left=pad_left, pad_right=pad_right)
        newline = self.__get_newline()

        # Create a text box around title and description
        headers = list()

        headers.append(h_border)
        headers.extend(self.wrap_text(title, pad_left, pad_right))
        headers.append(h_border)

        if description is not None:

            descript_lines = self.wrap_text(description, pad_left, pad_right)
            headers.extend(descript_lines)

        headers.extend([newline for i in range(2)])

        # Add general information
        ginfo = list()

        log_dt = dt.datetime.today().strftime('%A %B %d %Y | %I:%M:%S %p')

        ginfo.append(("Date and time", log_dt))
        ginfo.append(("Date and time ISO", dt.datetime.today().isoformat()))

        ginfo = self.kv_format(ginfo)
        headers.extend(self.section("General Information", ginfo))

        # Add info about python build
        py_build = list()
        py_build.append(("Version", "Python " + plt.python_version()))
        py_build.append(("Build", " ".join(plt.python_build())))
        py_build.append(("Compiler", plt.python_compiler()))

        py_build = self.kv_format(py_build)

        headers.extend(self.section("Python Information", py_build))

        # Add info about platform (or host OS)
        host = list()
        host.append(("Host", plt.platform() + " [" + plt.machine() + "]"))
        host.append(("Processor", plt.processor()))

        host = self.kv_format(host)

        headers.extend(self.section("System Information", host))

        return headers

    def section(self, title, items, subtitle=None, level=0, nlines_above=1,
                nlines_below=1):
        """Formats data for sectioning

        This method formats data in a human readable section. Each section will
        start with the section title, followed by a horizontal border. Level
        will indicate how many indentations should be added to the level, and
        items under a section are indented to the level of title plus one more

        Args:
            title (str):
                The title of the section

            items (collection of str):
                A collection of strings to be added under the section. Each
                string in the collection (set, list, tuple, etc.) is added on
                different lines order, with indentation of the title plus one
                more

            subtitle (str):
                A subtitle to be added under the section title. The subtitle is
                added immediately after the section title's horizontal border,
                followed by a newline. items are added afterwards. If None, no
                subtitles are added

            level (int):
                This dictates how many indentation should be added to the whole
                section. A value of zero is no indentation, with values above 0
                are indicated number of indentations

            nlines_above(int):
                Number of newlines above the section

            nlines_below(int):
                Number of newlines below the section

        Returns (list(str)):
            A list of strings containing the above formatted section
        """
        if level == 0:
            left_indent = ""

        else:
            left_indent = "".join([self.ind for i in range(level)])

        total_left_spacing = self.left_margin + left_indent

        section_headers = list()
        if nlines_above != 0:
            for _ in range(nlines_above):
                section_headers.append(self.__get_newline())

        title = self.wrap_text(title, pad_left=total_left_spacing, pad_right=self.right_margin)
        section_headers.extend(title)

        section_headers.append(self.__get_hborder(char="-",
                                                  pad_left=total_left_spacing,
                                                  pad_right=self.right_margin))

        # If subtitle exists, add in
        if subtitle is not None:
            subtitle = self.wrap_text(subtitle, pad_left=total_left_spacing,
                                      pad_right="     ")
            section_headers.extend(subtitle)

            section_headers.append(self.__get_newline,
                                   pad_left=total_left_spacing,
                                   pad_right=self.right_margin)

        # Now add item info
        section_headers.extend([total_left_spacing + self.ind + i
                                for i in items])

        # Add newlines below
        if nlines_below != 0:
            for _ in range(nlines_below):
                section_headers.append(self.__get_newline())

        return section_headers

    def kv_format(self, items, level=0, k_justify="left", split=" : ",
                  width=None):

        if width is None:
            width = self.max_width

        if level == 0:
            left_indent = ""

        else:
            left_indent = list()

            for _ in range(level):
                left_indent.append(self.ind)

            left_indent = "".join(left_indent)

        max_keywidth = max([len(i[0]) for i in items])

        result = list()
        for k, v in items:
            key_spacing = "".join([" " for _ in range(max_keywidth - len(k))])

            if k_justify == "left":
                key = self.left_margin + left_indent + k + key_spacing

            elif k_justify == "right":
                key = self.left_margin + left_indent + key_spacing + k

            sp_len = len(split)
            spaces_used = "".join([" " for _ in range(len(key) + sp_len)])

            value_wrapped = self.wrap_text(v, pad_right=self.right_margin,
                                           width=width - len(spaces_used))

            if len(value_wrapped) == 0:
                kv = key + split + value_wrapped[0]
                result.append(kv)

            else:
                for i in range(len(value_wrapped)):
                    if i == 0:
                        kv = key + split + value_wrapped[i]

                    else:
                        kv = spaces_used + key_spacing
                        kv += value_wrapped[i]

                    result.append(kv)

        return result

    def __rep_char(self, char, times=None, pad_left=None, pad_right=None):
        char_width = self.__get_textwidth(pad_left=pad_left,
                                          pad_right=pad_right)
        char_width = char_width['textwidth']

        if times is not None:
            repped_char = "".join([char for _ in range(times)])

            if len(repped_char) < char_width:
                n_spaces_add = char_width - len(repped_char)
                repped_char += "".join([" " for _ in range(n_spaces_add)])

        else:
            repped_char = "".join([char for _ in range(char_width)])

        if pad_left is not None:
            repped_char = pad_left + repped_char

        if pad_right is not None:
            repped_char = repped_char + pad_right

        return repped_char

    def __get_hborder(self, pad_left=None, pad_right=None, char=None):
        if char is None:
            char = self.hborder_char

        border = self.__rep_char(char, pad_left=pad_left, pad_right=pad_right)

        return border

    def __get_newline(self):
        return ""

    def __get_bborder(self, pad_left=None, pad_right=None, char=None):
        if char is None:
            char = self.bottom_char

        border = self.__rep_char(char, pad_left=pad_left, pad_right=pad_right)

        return border

    def __get_textwidth(self, pad_left=None, pad_right=None):
        result = dict()

        result['textwidth'] = self.max_width

        if pad_left is not None:
            result['textwidth'] -= len(pad_left)
            result['leftpad_width'] = len(pad_left)

        if pad_right is not None:
            result['textwidth'] -= len(pad_right)
            result['rightpad_width'] = len(pad_right)

        return result

    def wrap_text(self, text, pad_left=None, pad_right=None, width=None):
        if width is None:
            text_width = self.__get_textwidth(pad_left=pad_left,
                                              pad_right=pad_right)
            text_width = text_width['textwidth']

        else:
            text_width = width

        wrapped_text = list()

        for a_textline in textwrap.wrap(text, text_width):
            if len(a_textline) < text_width:
                for _ in range(text_width - len(a_textline)):
                    a_textline += " "

            if pad_left is not None:
                a_textline = pad_left + a_textline

            if pad_right is not None:
                a_textline = a_textline + pad_right

            wrapped_text.append(a_textline)

        return wrapped_text

    def has_header(self):
        result = None

        if len(self.resultheader) == 0:
            result = False

        else:
            result = True

        return result

    def has_lines(self):
        result = None

        if len(self.textlines) == 0:
            result = False

        else:
            result = True

            return result

    def __write_line(self, line, write_mode="a"):
        with open(self.file, write_mode) as f:
            f.writeline(line + "\n")

    def __write_lines(self, lines, write_mode="a"):
        lines = [i + "\n" for i in lines]

        with open(self.file, write_mode) as f:
            f.writelines(lines)
