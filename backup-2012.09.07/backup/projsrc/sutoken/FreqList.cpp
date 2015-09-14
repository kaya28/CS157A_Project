#include "StdAfx.h"
#include "FreqList.h"
#include "DataStorage.h"


bool CFreqList::exists(const string& tokenKey)
{
	bool bExists=false;
	map<string, CToken>::iterator it = mList.find(tokenKey);
	bExists = (it != mList.end()); 
	return bExists;
}

// Return docFreq of the token. If not exist, return -1.
long CFreqList::DocFreq(const string& tokenKey)
{
	long theDocFreq=-1;

	map<string, CToken>::iterator it = mList.find(tokenKey);
	bool bExists = (it != mList.end()); 
	if (bExists)
	{
		CToken& token = (it->second);
		theDocFreq=token.DocFreq();
	}
	return theDocFreq;
}

//used during retrieve from DB, does not assign new DBID
void CFreqList::InitAdd(CToken& token)
{
	mList.insert(map<string, CToken>::value_type(token.Key(),token));//insert created DB info into mList
	//map = key-value relation 
}

// Increment token count by 1, add if not exist.
void CFreqList::LogToken(CToken& pToken, CURLPage& pPage)
{
	map<string, CToken>::iterator it;
	it=mList.find(pToken.Key());
	if (it != mList.end())
	{	//already in list
		CToken& token = (it->second);
		token.AddURLID(pPage.ID());

		if (token.LastDocFound()==pToken.LastDocFound())
			token.IncrFreq(pToken.LastDocFound());
		else
			token.IncrFreq();

		// record the longest distance
		if (token.Distance() < pToken.Distance())
			token.Distance(pToken.Distance());
	} 
	else
	{	//not already in list

		//For new tokens, implement an elimination strategy here to
		//determine if we should create it or now.  If something is manually entered in DB, they it would be accounted
		//for in the above 'if' branch.

		pToken.IncrFreq();
		pToken.AddURLID(pPage.ID());

		mList.insert(map<string, CToken>::value_type(pToken.Key(),pToken));
	}
}

// Take input token count and add it into total count.
void CFreqList::mergeToken(CToken& newToken, long& nextDBID)
{
	map<string, CToken>::iterator it;
	it=mList.find(newToken.Key());
	if (it != mList.end())
	{	//already in list
		CToken& tokenExists = (it->second);
		if (tokenExists.LastDocFound() != newToken.LastDocFound())
			tokenExists.DocFreq(tokenExists.DocFreq() + newToken.DocFreq());
		tokenExists.Freq(tokenExists.Freq() + newToken.Freq()); 

		//merge URLList
		set<long>::iterator itNew = newToken.GetURLList().begin();
		while (itNew != newToken.GetURLList().end())
		{
			tokenExists.AddURLID(*itNew);
			itNew++;
		}
	} 
	else
	{	//not already in list, only add new ones if not in QueryMode.
		if (!QueryMode())
		{
			newToken.DBID(nextDBID++);
			mList.insert(map<string, CToken>::value_type(newToken.Key(),newToken));
		}
	}
}


void CFreqList::Serialize(P267_FILE *ofp, CDataStorage *pDB, long nTupleIndex, long totalTokenCount, long totalDocCount)
{
	if (QueryMode())
		QueryMatchup(ofp, pDB, nTupleIndex, totalTokenCount, totalDocCount);
	else
		DBSerialize(ofp, pDB, nTupleIndex, totalTokenCount, totalDocCount);
}

void CFreqList::QueryMatchup(P267_FILE *ofp, CDataStorage *pDB, long nTupleIndex, long totalTokenCount, long totalDocCount)
{
	fprintf(ofp,"Total rows: %d\n==========================\n", mList.size());
	//fprintf(ofp,"Token     \tTermFreq     \tTermFreq%%     \tDocFreq     \tDocFreq%%\n");
	fprintf(ofp,"%-50s   %-5s   %-10s   %-5s   %-10s\n", "Token", "TF", "TF%", "DF", "DF%");
	fprintf(ofp,"-------------------------------------------------------------------------------------------\n");
		
	map<string, CToken>::iterator it;
	double aDocFreqPercent;
	double aTermFreqPercent;
	char sSQL[1000];
	string csSQL;
	for(it=mList.begin(); it != mList.end(); it++)
	{                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        	
		CToken& aToken = (it->second);

		if (aToken.Dirty())
		{
			aDocFreqPercent = aToken.DocFreq()/(double)totalDocCount;
			aTermFreqPercent = aToken.Freq()/(double)totalTokenCount;
			fprintf(ofp,"%-50s   %-5d   %-10lf   %-5d   %-10lf\n", aToken.Key().c_str(), aToken.Freq(), aTermFreqPercent, aToken.DocFreq(), aDocFreqPercent);

			std::set<long>& listURL  = aToken.GetURLList();
			set<long>::iterator itURL = listURL.begin();
		}
	}}

void CFreqList::DBSerialize(P267_FILE *ofp, CDataStorage *pDB, long nTupleIndex, long totalTokenCount, long totalDocCount)
{
	fprintf(ofp,"Total rows: %d\n==========================\n", mList.size());
	//fprintf(ofp,"Token     \tTermFreq     \tTermFreq%%     \tDocFreq     \tDocFreq%%\n");
	fprintf(ofp,"%-50s   %-5s   %-10s   %-5s   %-10s\n", "Token", "TF", "TF%", "DF", "DF%");
	fprintf(ofp,"-------------------------------------------------------------------------------------------\n");
		
	map<string, CToken>::iterator it;
	double aDocFreqPercent;
	double aTermFreqPercent;
	char sSQL[1000];
	string csSQL;
	for(it=mList.begin(); it != mList.end(); it++)
	{                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        	
		CToken& aToken = (it->second);

		//only save if the DocFrequency > 1
		if (aToken.Dirty() && (aToken.DocFreq() >= DOCFREQMINCOUNT )) 
		{
			aDocFreqPercent = aToken.DocFreq()/(double)totalDocCount;
			aTermFreqPercent = aToken.Freq()/(double)totalTokenCount;
			//Nowrite fprintf(ofp,"%-50s   %-5d   %-10lf   %-5d   %-10lf\n", aToken.Key().c_str(), aToken.Freq(), aTermFreqPercent, aToken.DocFreq(), aDocFreqPercent);

			if (!pDB->sp_UpdateConcept(aToken, nTupleIndex))
			{
				printf("**CFrequList::DBSerialize Error pDB->sp_UpdateConcept: %s\n", aToken.getToken());
			}

			std::set<long>& listURL  = aToken.GetURLList();
			set<long>::iterator itURL = listURL.begin();

			while (itURL != listURL.end())
			{
				sprintf(sSQL, "exec sp_ConnectConceptToURL %d, %d", aToken.DBID(), *itURL);
				csSQL = sSQL;
				if (!pDB->ExecuteSQL(csSQL))
				{
					printf("** Error exec sp_ConnectConceptToURL: %s\n", sSQL);
				}

				itURL++;
			}
		}
	}
}

// percent are 0.0 to 1.0
bool CFreqList::pruneDocFreq(P267_FILE *ofp, double bottomPercent, double topPercent, long totalDocCount)
{
	//map<string, CToken>::iterator it;
	map<string, CToken>::iterator it;
	deque<string> lsToPrune;
	double aPercent;

	// find out which to prune
	for(it=mList.begin(); it != mList.end(); it++)
	{
		CToken& t = (it->second);
		aPercent = t.DocFreq()/(double)totalDocCount;
		if ( (aPercent < bottomPercent) || (aPercent > topPercent) ) // discard too little, discard too much
//		if (aPercent < bottomPercent) 
		{
			lsToPrune.push_back(t.Token());
			if (ofp)
			{
				fprintf(ofp,"pruneDocFreq removing [%s] %lf\n", t.Token().c_str(), aPercent);
			}
		}
	}

	printf("pruneDocFreq removing %d of %d = %d\n", lsToPrune.size(), mList.size(), mList.size()-lsToPrune.size() );
		
	// Now do the actual pruning
	deque<string>::iterator pruneIt = lsToPrune.begin();
	while (pruneIt != lsToPrune	.end())
	{
		mList.erase(*pruneIt);
		pruneIt++;
	}

	lsToPrune.clear();

	return true;
}

// prune term frequency within a file
bool CFreqList::pruneTermFreq(double bottomPercent, double topPercent)
{
	map<string, CToken>::iterator it;
	deque<string> lsToPrune;
	double aPercent;
		
	double totalTermCount = (double) mList.size();

	// find out which to prune
	for(it=mList.begin(); it != mList.end(); it++)
	{
		CToken& t = (it->second);
		aPercent = t.Freq()/(double)totalTermCount;
		//if (isStopWord(t.Token()) || (aPercent < bottomPercent) || (aPercent > topPercent) )
		if ((aPercent < bottomPercent) || (aPercent > topPercent) )
		{
			lsToPrune.push_back(t.Token());

			if (SHOWPRUNE)
				printf("pruneTermFreq removing: %s %lf\n", t.Token().c_str(), aPercent);
		}
	}

	//printf("pruneTermFreq removing %d of %d = %d\n", lsToPrune.size(), mList.size(), mList.size()-lsToPrune.size() );

	// Now do the actual pruning
	//printf("\npruneTermFreq %d tokens.\n", lsToPrune.size());

	deque<string>::iterator pruneIt = lsToPrune.begin();
	while (pruneIt != lsToPrune.end())
	{
		mList.erase(*pruneIt);
		pruneIt++;
	}

	lsToPrune.clear();

	return true;
}
