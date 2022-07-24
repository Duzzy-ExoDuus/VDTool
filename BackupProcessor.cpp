#include <iostream>
#include <filesystem>
#include <fstream>
#include <future>
#include <thread>
#include <memory>
#include <map>
#include "BackupProcessor.h"
#include "core/file_handler.h"
#include "core/crc32.h"
#include "core/compression.h"

using namespace std;
using namespace Aws::Utils::Json;

#define VIXDISKLIB_VERSION_MAJOR 6
#define VIXDISKLIB_VERSION_MINOR 8
#define SECTOR_CHUNK 2048

int GetBackupBlockData(BackupStorage *backupStorage, string backupId, int partId, string key, int partIndex, char *buffer)
{
	size_t cmpBufferSize = 2 * MB_BLOCK_SIZE;
	char* cmpBuffer = (char*)malloc(cmpBufferSize);

	int size = backupStorage->GetBackupBlockData(backupId, partId, key, { partIndex }, cmpBuffer);

	int out_data_size = cmpBufferSize;
	int bytes = decompress_raw_data(cmpBuffer, size, buffer, out_data_size);

	free(cmpBuffer);

	return bytes;
}

BackupProcessor::BackupProcessor(BackupStorage* backupStorage,
	string backupId) :
	m_backupStorage(backupStorage),
	m_backupId(backupId)
{
}

int BackupProcessor::ReadChangedDiskAreas()
{
	WCHAR buff[FILENAME_MAX];
	int bytes = GetModuleFileName(NULL, buff, FILENAME_MAX);
	string exe = "vdtool.exe";
	wstring ws(buff);
	string path(ws.begin(), ws.end() - exe.length());

	path += "cbt.data";

	file_handler handler = open_file_data(path.c_str(), "rb");
	char* buffer = (char*)malloc(sizeof(ChangedDiskArea));
	memset(buffer, 0, sizeof(ChangedDiskArea));
	bytes = read_file_data(&handler, buffer, sizeof(UINT64));

	if (bytes == 0)
	{
		return 0;
	}

	UINT64* ptr = (UINT64*)(buffer);
	UINT64 count = *ptr;
	m_changedDiskAreas.clear();

	for (UINT64 i = 0; i < count; i++)
	{
		int bytes = read_file_data(&handler, buffer, sizeof(ChangedDiskArea));

		if (bytes == 0)
		{
			cout << "ChangedDiskAreas file error" << endl;
			return ERROR_CODE;
		}

		ChangedDiskArea* diskAreaPtr = (ChangedDiskArea*)buffer;
		m_changedDiskAreas.push_back(*diskAreaPtr);
	}

	free(buffer);
	close_file(&handler);

	return 0;
}

int BackupProcessor::QueryAllocatedBlocks(VixDiskLibHandle& handle)
{
	VixDiskLibInfo* diskInfo = (VixDiskLibInfo*)malloc(sizeof(VixDiskLibInfo));
	memset(diskInfo, 0, sizeof(VixDiskLibInfo));
	VixError vixError = VixDiskLib_GetInfo(handle, &diskInfo);

	if (vixError != VIX_OK)
	{
		cout << "Disk GetInfo error, code: " << vixError << endl;

		return vixError;
	}

	VixDiskLibBlockList* blockList = NULL;
	UINT64 capacity = diskInfo->capacity;
	UINT64 chunkSize = VIXDISKLIB_MIN_CHUNK_SIZE;
	UINT64 numChunk = capacity / chunkSize;
	UINT64 offset = 0;

	VixDiskLib_FreeInfo(diskInfo);

	while (numChunk > 0) 
	{
		uint64 numChunkToQuery;

		if (numChunk > VIXDISKLIB_MAX_CHUNK_NUMBER) 
		{
			numChunkToQuery = VIXDISKLIB_MAX_CHUNK_NUMBER;
		}
		else 
		{
			numChunkToQuery = numChunk;
		}

		blockList = NULL;

		vixError = VixDiskLib_QueryAllocatedBlocks(handle, offset, numChunkToQuery * chunkSize, chunkSize, &blockList);

		if (vixError != VIX_OK)
		{
			cout << "Query allocated blocks error, code: " << vixError << endl;

			VixDiskLib_FreeInfo(diskInfo);
			VixDiskLib_FreeBlockList(blockList);

			return vixError;
		}

		for (int i = 0; i < blockList->numBlocks; i++)
		{
			VixDiskLibBlock block = blockList->blocks[i];

			ChangedDiskArea diskArea;
			diskArea.start = block.offset * VIXDISKLIB_SECTOR_SIZE;
			diskArea.length = block.length * VIXDISKLIB_SECTOR_SIZE;

			m_changedDiskAreas.push_back(diskArea);
		}

		numChunk -= numChunkToQuery;
		offset += numChunkToQuery * chunkSize;

		VixDiskLib_FreeBlockList(blockList);
	}

	return 0;
}

int BackupProcessor::BackupData(InputParams& params)
{
	VixError vixError = VixDiskLib_InitEx(VIXDISKLIB_VERSION_MAJOR,
										  VIXDISKLIB_VERSION_MINOR,
										  NULL, NULL, NULL,
										  params.libDir.c_str(),
										  params.cfgFile.c_str());

	if (vixError != VIX_OK)
	{
		cout << "VixDiskLib init error, code: " << vixError << endl;

		return BackupTaskWithError(vixError);
	}

	char *snapRef = NULL;

	if (!params.snapshotRef.empty())
	{
		snapRef = (char *)params.snapshotRef.c_str();
	}

	VixDiskLibConnection connection;

	vixError = VixDiskLib_ConnectEx(&params.cnxParams, TRUE, snapRef, NULL, &connection);

	if (vixError != VIX_OK)
	{
		cout << "VixDiskLib connect error, code: " << vixError << endl;
		VixDiskLib_Exit();

		return BackupTaskWithError(vixError);
	}

	VixDiskLibHandle handle;

	uint32 flags = VIXDISKLIB_FLAG_OPEN_UNBUFFERED | VIXDISKLIB_FLAG_OPEN_READ_ONLY;

	vixError = VixDiskLib_Open(connection, params.vmdk.c_str(), flags, &handle);

	if (vixError != VIX_OK)
	{
		cout << "VixDiskLib open error, code: " << vixError << endl;
		VixDiskLib_Disconnect(connection);
		VixDiskLib_Exit();

		return BackupTaskWithError(vixError);
	}

	int result = 0;

	if (params.fullBackup)
	{
		result = QueryAllocatedBlocks(handle);
	}
	else
	{
		result = ReadChangedDiskAreas();
	}

	if (result != 0)
	{
		VixDiskLib_Close(handle);
		VixDiskLib_Disconnect(connection);
		VixDiskLib_Exit();

		return BackupTaskWithError(VIX_E_FAIL);
	}

	UINT64 lastSectorOffset = 0;

	for (UINT64 i = 0; i < m_changedDiskAreas.size(); i++)
	{
		ChangedDiskArea entry = m_changedDiskAreas[i];
		lastSectorOffset = max(lastSectorOffset, entry.start + entry.length);
	}

	UINT64 blockCount = (UINT64)ceil((float)lastSectorOffset / (float)MB_BLOCK_SIZE);
	UINT16 sectorSize = VIXDISKLIB_SECTOR_SIZE;
	size_t blockDataMetadataSize = 2 * sizeof(UINT16) + (MB_BLOCK_SIZE / sectorSize) * sizeof(UINT16);
	size_t blockDataSize = blockDataMetadataSize + MB_BLOCK_SIZE;

	char* blockBuffer = (char*)malloc(blockDataSize);
	memset(blockBuffer, 0, blockDataSize);
	memcpy(blockBuffer, (void*)&sectorSize, sizeof(UINT16));

	size_t cmpBufferSize = MB_BLOCK_SIZE + blockDataMetadataSize + CMP_SIZE;
	char* cmpBlockBuffer = (char*)malloc(cmpBufferSize * UploadBatchSize);
	memset(cmpBlockBuffer, 0, cmpBufferSize * UploadBatchSize);
	int cmpBufferOffsetIndex = -1;

	BackupMetaData backupMetaData = m_backupStorage->GetBackupMetaData(m_backupId);

	for (UINT64 i = 0; i < blockCount; i++)
	{
		UINT64 start = i * MB_BLOCK_SIZE;
		UINT64 end = (i + 1) * MB_BLOCK_SIZE;

		if (i + 1 == blockCount)
		{
			end = lastSectorOffset;
		}

		vector<UINT64> sectorIndices;
		UINT64 sectorBufferOffset = 0;

		for (UINT64 j = 0; j < m_changedDiskAreas.size(); j++)
		{
			ChangedDiskArea entry = m_changedDiskAreas[j];

			UINT64 areaStart = entry.start;
			UINT64 areaEnd = areaStart + entry.length;

			if (areaEnd < start || areaStart > end)
			{
				continue;
			}

			UINT64 intersectionStart = max(start, areaStart);
			UINT64 intersectionEnd = min(end, areaEnd);
			UINT64 startSector = intersectionStart / sectorSize;
			UINT64 endSector = intersectionEnd / sectorSize;
			UINT64 sectorNum = endSector - startSector;
		
			UINT64 blockSectorStart = i * MB_BLOCK_SIZE / sectorSize;
			UINT64 blockSectorOffset = startSector - blockSectorStart;

			for (UINT64 k = 0; k < sectorNum; k++)
			{
				sectorIndices.push_back(blockSectorOffset + k);
			}

			char *ptr = blockBuffer + blockDataMetadataSize + sectorBufferOffset;

			vixError = VixDiskLib_Read(handle, startSector, sectorNum, (uint8 *)ptr);

			sectorBufferOffset += (sectorNum * sectorSize);

			if (vixError != VIX_OK)
			{
				VixDiskLib_Close(handle);
				VixDiskLib_Disconnect(connection);
				VixDiskLib_Exit();

				return BackupTaskWithError(vixError);
			}
		}

		UINT16 sectorCount = sectorIndices.size();

		if (sectorCount == 0)
		{
			continue;
		}

		memcpy(blockBuffer + sizeof(UINT16), (void*)&sectorCount, sizeof(UINT16));

		for (UINT64 k = 0; k < sectorIndices.size(); k++)
		{
			UINT16 sectorId = sectorIndices[k];
			memcpy(blockBuffer + 2 * sizeof(UINT16) + k * sizeof(UINT16), (void*)&sectorId, sizeof(UINT16));
		}

		UINT64 position = i * MB_BLOCK_SIZE;
		UINT64 partId = position / DATA_BUFFER_SIZE;
		UINT64 blockId = (position - partId * DATA_BUFFER_SIZE) / MB_BLOCK_SIZE;
		size_t blockUploadSize = blockDataMetadataSize + (uint64_t)sectorCount * sectorSize;

		char* dataPtr = blockBuffer + blockDataMetadataSize;
		size_t dataSize = (uint64_t)sectorCount * sectorSize;
		uint64_t crc = sse42_crc32((uint64_t *)dataPtr, dataSize);
		string item = to_string(partId + 1) + "/" + to_string(blockId + 1);

		if (crc == 0)
		{
			sectorCount = 0;
			blockUploadSize = 2 * sizeof(UINT16);
			memcpy(blockBuffer + sizeof(UINT16), (void*)&sectorCount, sizeof(UINT16));
		}

		cmpBufferOffsetIndex = m_backupStorage->GetFreeBufferOffsetIndex();
		char* bufferOffset = cmpBlockBuffer + cmpBufferOffsetIndex * cmpBufferSize;

		size_t out_data_size = 0;
		compress_raw_data(blockBuffer, blockUploadSize + CMP_SIZE, (void*)bufferOffset, out_data_size);

		m_backupStorage->UploadBackupSectorDataAsync(m_backupId, item, backupMetaData.encryptionKey, bufferOffset, cmpBufferOffsetIndex, out_data_size);
	}

	m_backupStorage->WaitForAllUploadTasksToComplete();

	free(blockBuffer);
	free(cmpBlockBuffer);

	VixDiskLib_Close(handle);
	VixDiskLib_Disconnect(connection);
	VixDiskLib_Exit();

	backupMetaData.status = BackupStatus::Complete;
	backupMetaData.encryptionKey = "";
	m_backupStorage->UploadBackupMetaData(m_backupId, backupMetaData);

	return 0;
}

VixError BackupProcessor::BackupTaskWithError(VixError vixError)
{
	BackupMetaData backupMetaData;
	backupMetaData.encryptionKey = "";
	backupMetaData.status = BackupStatus::Error;
	m_backupStorage->UploadBackupMetaData(m_backupId, backupMetaData);

	return vixError;
}

VixError BackupProcessor::RestoreTaskWithError(VixError vixError)
{
	RestoreTaskMetaData restoreMetadata;
	restoreMetadata.encryptionKey = "";
	restoreMetadata.status = RestoreStatus::RestoreError;
	m_backupStorage->UploadRestoreTaskMetaData(restoreMetadata);

	return vixError;
}

int BackupProcessor::RestoreData(InputParams& params, string volumeId, string restoreId)
{
	VixError vixError = VixDiskLib_InitEx(VIXDISKLIB_VERSION_MAJOR, 
										  VIXDISKLIB_VERSION_MINOR,
										  NULL, NULL, NULL,
										  params.libDir.c_str(),
										  params.cfgFile.c_str());

	if (vixError != VIX_OK)
	{
		cout << "VixDiskLib init error, code: " << vixError << endl;

		return RestoreTaskWithError(vixError);
	}

	VixDiskLibConnection connection;

	vixError = VixDiskLib_Connect(&params.cnxParams, &connection);

	if (vixError != VIX_OK)
	{
		cout << "VixDiskLib connect error, code: " << vixError << endl;
		VixDiskLib_Exit();

		return RestoreTaskWithError(vixError);
	}

	VixDiskLibHandle handle;

	uint32 flags = VIXDISKLIB_FLAG_OPEN_UNBUFFERED;

	vixError = VixDiskLib_Open(connection, params.vmdk.c_str(), flags, &handle);

	if (vixError != VIX_OK)
	{
		cout << "VixDiskLib open error, code: " << vixError << endl;
		VixDiskLib_Disconnect(connection);
		VixDiskLib_Exit();

		return RestoreTaskWithError(vixError);
	}

	VolumeMetaData metadata = m_backupStorage->GetVolumeMetaData(volumeId);

	map<int, map<uint32_t, vector<string>>> partitionIndices;

	for (int i = 0; i < metadata.backupIds.size(); i++)
	{
		string backupId = metadata.backupIds[i];

		for (int partId = 0; partId < params.volumeSize; partId++)
		{
			vector<int> objects;

			int result = m_backupStorage->ListObjects(backupId, partId, objects);

			if (result != 0)
			{
				VixDiskLib_Close(handle);
				VixDiskLib_Disconnect(connection);
				VixDiskLib_Exit();

				return RestoreTaskWithError(result);
			}

			for (auto iter = objects.begin(); iter != objects.end(); iter++)
			{
				partitionIndices[partId][*iter].push_back(backupId);
			}
		}

		if (backupId == m_backupId)
		{
			break;
		}
	}

	const int concurrentThreads = 10;
	size_t bufferSize = 2 * MB_BLOCK_SIZE;
	char* buffer = (char*)malloc(bufferSize * concurrentThreads);
	UINT16 sectorSize = VIXDISKLIB_SECTOR_SIZE;
	UINT16 sectorsInMbBlock = MB_BLOCK_SIZE / sectorSize;
	size_t blockDataMetadataSize = 2 * sizeof(UINT16) + sectorsInMbBlock * sizeof(UINT16);

	RestoreTaskMetaData restoreMetadata = m_backupStorage->GetRestoreTaskMetaData(restoreId);
	string encryptionKey = restoreMetadata.encryptionKey;

	for (int partId = 0; partId < params.volumeSize; partId++)
	{
		auto item = partitionIndices.find(partId);

		if (item == partitionIndices.end())
		{
			continue;
		}

		auto blocks = partitionIndices[partId];

		map<string, vector<int>> backupBlockIndices;

		for (auto iter = blocks.begin(); iter != blocks.end(); iter++)
		{
			auto backupIds = iter->second;

			for (int i = 0; i < backupIds.size(); i++)
			{
				string backupId = backupIds[i];
				backupBlockIndices[backupId].push_back(iter->first);
			}
		}

		for (auto iter = backupBlockIndices.begin(); iter != backupBlockIndices.end(); iter++)
		{
			string backupId = iter->first;
			vector<int> partIndices = iter->second;

			if (partIndices.size() == 0)
			{
				continue;
			}

			size_t offset = 0;
			size_t threadGroups = (partIndices.size() + concurrentThreads - 1) / concurrentThreads;

			for (int i = 0; i < threadGroups; i++)
			{
				memset(buffer, 0, bufferSize * concurrentThreads);

				size_t indexNum = min((size_t)(partIndices.size() - offset), concurrentThreads);
				vector<future<int>> tasks;

				for (int k = 0; k < indexNum; k++)
				{
					char *bufferOffset = buffer + k * bufferSize;
					int partIndex = partIndices[offset + k];
					tasks.push_back(async(GetBackupBlockData, m_backupStorage, backupId, partId, encryptionKey, partIndex, bufferOffset));
				}

				for (auto &task : tasks)
				{
					task.get();
				}

				for (int j = 0; j < indexNum; j++)
				{
					char *bufferOffset = buffer + j * bufferSize;
					int partIndex = partIndices[offset + j];

					UINT16 sectorNum = *(UINT16*)(bufferOffset + sizeof(UINT16));

					UINT64 numSectorsToWrite = 0;
					UINT16 lastSectorIndex = 0;
					INT64 startSector = -1;
					char* dataBlockPtr = bufferOffset + blockDataMetadataSize;

					for (UINT16 k = 0; k < sectorNum; k++)
					{
						char* ptr = bufferOffset + 2 * sizeof(UINT16) + k * sizeof(UINT16);
						UINT16 sectorIndex = *(UINT16*)ptr;

						if (startSector < 0)
						{
							startSector = sectorIndex;
						}

						if (sectorIndex - lastSectorIndex > 1)
						{
							if (numSectorsToWrite > 0)
							{
								UINT64 startSectorToWrite = (partId * 1024 + partIndex) * sectorsInMbBlock + startSector;
								vixError = VixDiskLib_Write(handle, startSectorToWrite, numSectorsToWrite, (uint8 *)(dataBlockPtr + startSector * sectorSize));
								ProcessWriteResult(vixError, buffer, handle, connection);
								numSectorsToWrite = 0;
							}

							startSector = sectorIndex;
						}

						numSectorsToWrite++;

						lastSectorIndex = sectorIndex;
					}

					if (numSectorsToWrite > 0)
					{
						UINT64 startSectorToWrite = (partId * 1024 + partIndex) * sectorsInMbBlock + startSector;
						vixError = VixDiskLib_Write(handle, startSectorToWrite, numSectorsToWrite, (uint8 *)(dataBlockPtr + startSector * sectorSize));
						ProcessWriteResult(vixError, buffer, handle, connection);
					}
				}

				offset += indexNum;
			}
		}
	}

	free(buffer);

	restoreMetadata.encryptionKey = "";
	restoreMetadata.restoreId = restoreId;
	restoreMetadata.status = RestoreStatus::RestoreComplete;
	m_backupStorage->UploadRestoreTaskMetaData(restoreMetadata);

	VixDiskLib_Close(handle);
	VixDiskLib_Disconnect(connection);
	VixDiskLib_Exit();

	return 0;
}

VixError BackupProcessor::ProcessWriteResult(VixError vixError, char* buffer, VixDiskLibHandle handle, VixDiskLibConnection connection)
{
	if (vixError != VIX_OK)
	{
		cout << "VixDiskLib_Write block error, code: " << vixError << endl;
		free(buffer);

		VixDiskLib_Close(handle);
		VixDiskLib_Disconnect(connection);
		VixDiskLib_Exit();

		return RestoreTaskWithError(vixError);
	}

	return vixError;
}