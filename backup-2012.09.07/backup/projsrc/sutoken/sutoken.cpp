// sutoken.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "Config.h"
#include "TokenizerUtils.h"
#include "DataStorage.h"

int _tmain(int argc, char* argv[])
{
	CBenchmark stopWatch;	
	CConfig theConfig;
	CDataStorage theDB;
	
	//Windows specific init
	HRESULT hr = CoInitialize(NULL);
	//theDB.TestConnection();

	initLegals();
	//initStopWords();

	long totalCount = 0;
	if (theConfig.parseConfig())
	{
		if (theDB.OpenConnection(theConfig.SQLDSN()))		
		{
			CThreadManager threadManager;
			
			threadManager.InitFromDB(&theDB); // This is before parseCommandLine so that command parameters can overwrite DB config
			//read token data from DB 

			if (theConfig.parseCommandLine(argc, argv))
			{
				threadManager.QueryMode(theConfig.QueryMode());

				if (theConfig.QueryMode())
				{
					char sFile[500];
					CURLPage aPage;

					// Query mode
					threadManager.InitThreads(theConfig.ThreadCount());
					while (true)
					{
						printf("\n\nEnter file name to categorize: ");
						fgets(sFile, 500, stdin);
						string trimmed = sFile;
						trimmed.erase(trimmed.find_last_not_of(" \n\r\t")+1); 
						if (trimmed.empty())
							continue;

						printf("You entered file %s\n", trimmed.c_str());

						aPage.Set(-1,trimmed,trimmed);
						aPage.URL(trimmed);
						theConfig.InputFileList().push_back(aPage);

						threadManager.SetInputFileList(&theConfig.InputFileList());

						threadManager.openMasterTokensFile(theConfig.OutfilePrefix());

						for (int i=1; i <= theConfig.NTupleCount(); i++)
						{
							threadManager.parseNtuple(i, theConfig.OutfilePrefix());

							if (i==1)
								printf("\nElapsed %f seconds. Found %d tokens.\n", stopWatch.ElapsedSecs(), threadManager.MasterTokenCount());
							else
								printf("\nElapsed %f seconds.\n", stopWatch.ElapsedSecs());

							if (i == theConfig.NTupleCount())
							{
								threadManager.Serialize(i,theConfig.OutfilePrefix()); // need to output last one.
							}
						}

						threadManager.closeMasterTokensFile();
		
						theConfig.InputFileList().clear();
						Sleep(250); // for threads to end.

						totalCount = threadManager.MasterTokenCount();
					}

					threadManager.EndThreads();
				}
				else
				{
					// Analysis mode
					threadManager.SetInputFileList(&theConfig.InputFileList());
					threadManager.InitThreads(theConfig.ThreadCount());
					const string& outputFileprefix = theConfig.OutfilePrefix();
					threadManager.openMasterTokensFile(outputFileprefix);

					for (int i=1; i <= theConfig.NTupleCount(); i++)
					{
						threadManager.parseNtuple(i, theConfig.OutfilePrefix());

						if (i==1)
							printf("\nElapsed %f seconds. Found %d tokens.\n", stopWatch.ElapsedSecs(), threadManager.MasterTokenCount());
						else
							printf("\nElapsed %f seconds.\n", stopWatch.ElapsedSecs());

						if (i == theConfig.NTupleCount())
						{
							threadManager.Serialize(i,theConfig.OutfilePrefix()); // need to output last one.
						}
					}

					//threadManager.toFiles(theConfig.OutfilePrefix());

					// prune DocFrequency in DB
					threadManager.pruneDBDocFrequency();

					threadManager.closeMasterTokensFile();
		
					threadManager.EndThreads();
					Sleep(250); // for threads to end.

					totalCount = threadManager.MasterTokenCount();
				}

			}
		} 
		else
		{
			printf("*** Error: Failed to open DB conncection\n");	
		}

		theDB.CloseConnection();
	}
	
	printf("\nProgram executed in %f seconds. Found %d tokens. Speed = %lf tokens/sec\n", stopWatch.ElapsedSecs(), totalCount, totalCount/(double)stopWatch.ElapsedSecs() );

	return 0;
} 

