#if defined(_MSC_VER)
	#include <windows.h>
#endif

#include <stdio.h>
#include <string.h>

#if defined(__GNUC__)
	#include <unistd.h>
	#include <sys/types.h>
	#include <sys/stat.h>
	#include <fcntl.h>
#endif

struct file_handler
{
	int file_descriptor;

	void *handle;

	file_handler()
	{
		file_descriptor = 0;
		handle = NULL;
	}
};

void log_error()
{
#if defined(_MSC_VER)
	DWORD errorMessageId = GetLastError();

	if (errorMessageId > 0)
	{
		LPSTR messageBuffer = nullptr;
		size_t size = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
			NULL, errorMessageId, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);

		string message(messageBuffer, size);

		LocalFree(messageBuffer);

		cout << message << endl;
	}
#endif
}

file_handler open_file_data(const char *file, const char *mode)
{
	file_handler handler;

#if defined(__GNUC__)
	if (mode == "rb")
	{
		handler.file_descriptor = open(file, O_RDONLY);
	}
	else if (mode == "wb")
	{
		handler.file_descriptor = open(file, O_RDWR);
	}
#else
	BOOL readonly = mode == "rb";
	DWORD dwDesiredAccess = readonly ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE;
	DWORD dwSharedMode = readonly ? FILE_SHARE_READ : FILE_SHARE_READ | FILE_SHARE_WRITE;

	handler.handle = CreateFileA(file, dwDesiredAccess, dwSharedMode, NULL, OPEN_EXISTING, 0, NULL);

	log_error();

	if (!readonly)
	{
		DWORD dwBytesReturned;
		bool bRes = DeviceIoControl
		(
			handler.handle,
			FSCTL_LOCK_VOLUME,
			NULL,
			0,
			NULL,
			0,
			&dwBytesReturned,
			NULL
		);

		log_error();
	}

#endif

	return handler;
}

void seek_file(file_handler *handler, long offset)
{
int result = 0;

#if defined(__GNUC__)
	result = lseek(handler->file_descriptor, (off_t)offset, SEEK_SET);
#else
	DWORD dwPtr = SetFilePointer(handler->handle, offset, NULL, FILE_BEGIN);

	if (dwPtr == INVALID_SET_FILE_POINTER)
	{
		log_error();
	}

#endif
}

int read_file_data(file_handler *handler, void *data, int size)
{
	int result = 0;

#if defined(__GNUC__)
	result = (int)read(handler->file_descriptor, data, size);
#else
	DWORD nRead = 0;
	BOOL res = ReadFile(handler->handle, data, size, &nRead, NULL);
	result = (int)nRead;

	log_error();

#endif

	return result;
}

int write_file_data(file_handler *handler, void *data, int size)
{
	int result = 0;

#if defined(__GNUC__)
	result = (int)write(handler->file_descriptor, data, size);
#else
	DWORD nWritten = 0;
	BOOL res = WriteFile(handler->handle, data, size, &nWritten, NULL);
	result = (int)nWritten;

	log_error();

#endif

	return result;
}

void close_file(file_handler* handler)
{
#if defined(__GNUC__)
	close(handler->file_descriptor);
#else
	CloseHandle(handler->handle);
#endif
}