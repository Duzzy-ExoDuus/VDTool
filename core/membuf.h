#include <iostream>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

struct membuf : streambuf
{
	membuf(char *begin, char *end) : begin(begin), end(end)
	{
		this->setg(begin, begin, end);
	}

	virtual pos_type seekoff(off_type off, ios_base::seekdir dir, ios_base::openmode which = ios_base::in) override
	{
		if (dir == ios_base::cur)
		{
			gbump((int)off);
		}
		else if (dir == ios_base::end)
		{
			setg(begin, end + off, end);
		}
		else if (dir == ios_base::beg)
		{
			setg(begin, begin + off, end);
		}

		return gptr() - eback();
	}

	virtual pos_type seekpos(streampos pos, ios_base::openmode mode) override
	{
		return seekoff(pos - pos_type(off_type(0)), ios_base::beg, mode);
	}

	char *begin, *end;
};