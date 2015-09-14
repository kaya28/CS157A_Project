#include "StdAfx.h"
#include "Config.h"
#include "TokenizerUtils.h"

CConfig::CConfig(void) 
{
	mOutfilePrefix="";
	mNTupleCount =MAX_TUPLE_COUNT;
	mThreadCount=2;
}


CConfig::~CConfig(void)
{
}


bool CConfig::parseCommandLine(int argc, char** argv)
{
	bool brc=false;
	if ((argc > 1) && (argv[1][0]!='?')) 
	{
		while (argc>1)
		{
			++argv;
			--argc;
			if (strncmp(*argv,"-o",2)==0)
			{
				//specify out file, currently this -o argument must be the first one, if used.
				//no space is allowed between the -o and the filename.
				mOutfilePrefix=&(*argv)[2];
				--argc;
				++argv;

				if (strncmp(*argv,"-n",2)==0)
				{
					//specify number of levels, currently this -d argument must be the second one, if used.
					//no space is allowed between the -d and the number.
					mNTupleCount = MIN(MAX_TUPLE_COUNT, atol((const char*)&(*argv)[2]) );
			
					--argc;
					++argv;
				} 

				if (strncmp(*argv,"-t",2)==0)
				{
					//specify number of threads, no space is allowed between the -t and the number.
					mThreadCount = MIN(MAX_THREAD_COUNT, atol((const char*)&(*argv)[2]) );
			
					--argc;
					++argv;
				} 

				brc=true;

				//the rest of arguments are input file names
				char sfilename[512];
				while (argc-- >0)
				{
					P267_FILE *fpFilelist=NULL;
					char *s=*argv++;

					if (strncmp(s,"-l",2)==0)
					{
						// this is a filelist, use it to get filenames to parse.
						s=&s[2];
						if ((fpFilelist = P267_open(s, P267_IOREAD))!=NULL)
						{
							char c;
							int idx=0;
							memset(sfilename,0,512);

							while((c = P267_getcf(fpFilelist)) != EOF) 
							{
								if ((c!=P267_NEWLINE) && (c!=P267_CARRIAGERETURN) && (c!=P267_ENDFILE))
								{
									sfilename[idx++]=c;
								} 
								else 
								{
									//Now we have a complete filename from one
									string trimmed = sfilename;
									trimmed.erase(trimmed.find_last_not_of(" \n\r\t")+1); 
									if (!trimmed.empty())
									{
										mInputFilelist.push_back(trimmed);
									}
									idx=0;
									memset(sfilename,0,512);
								}
							}

							P267_close(fpFilelist);
						}
					}
					else
					{
						mInputFilelist.push_back(s);
					}
				}
			} 
		}
	}

	if (brc)
	{
		//printf what user has provided
		printf("\n**** Super Tokenizer ****\n");
		printf("Prefix output files with '%s'\n", mOutfilePrefix.c_str());
		printf("Analyze up to %d tuples\n", mNTupleCount);
		printf("Spawn %d threads\n", mThreadCount);
		printf("Keep Term Frequency: %.2f%% to %.2f%%\n", TERMFREQ_FROM*100, TERMFREQ_TO*100);
		printf("Keep Doc Frequency: %.2f%% to %.2f%%\n", DOCFREQ_FROM*100, DOCFREQ_TO*100);
		printf("Window size: %d\n", WINDOW_SIZE);
		printf("Paragraph size: %d to %d\n", MINPARAGRAPH_SIZE, MAXPARAGRAPH_SIZE);
		printf("Number of files: %d\n\n", mInputFilelist.size());
	}
	else 
	{
		printf("Usage: %s -oOutFile [-nTupleCount] [-tThreadCount] [-lFileList] [InputFiles...]\n",argv[0]);
	}

	return brc;
}
