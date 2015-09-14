#include "StdAfx.h"
#include "Token.h" 
#include "porterStemmer.h"

void CToken::set(const char* t, long pos) 
{ 
	mToken=t; 
	toLowerCase(mToken); 
	mToken.erase(mToken.find_last_not_of(" \n\r\t'?:;.,")+1); // delete special chars such as \n, \t, "", etc.
	mTokenOrig=mToken;//deleted one set to TokensOrign 
	mFirstPos= pos; 
	mDistance = 0;
}

void CToken::set(const char* t, const char* tOrig, long pos)
{ 
	set(t,pos);
	mTokenOrig=tOrig;
}
	
CToken::CToken() 
{
	mFreq = mDocFreq = mFirstPos = mDistance = mDBID = 0;
	mDirty = false;
	mStemmer = NULL;
	mPerm = 0;
}

// copy constructor
CToken::CToken(const CToken& c)
{ 
	mToken = c.mToken; 
	mTokenOrig = c.mTokenOrig;
	mFirstPos=c.mFirstPos;
	mDistance=c.mDistance;
	mFreq = c.mFreq;
	mDocFreq = c.mDocFreq;
	mLastDocFound = c.mLastDocFound;
	mDBID = c.mDBID;
	mDirty = c.mDirty;
	mStemmer = NULL;
	mPerm = c.mPerm;

	//deep clone of list 
	std::set<long>::iterator itURL = c.mURLList.begin();
	while (itURL!= c.mURLList.end())
	{
		mURLList.insert(*itURL);
		itURL++;
	}
}

CToken::~CToken() 
{
	mFreq = mDocFreq = mFirstPos = mDBID = 0;
	if (mStemmer)
		free_stemmer(mStemmer);
}

void CToken::clear() 
{ 
	mFreq = mDocFreq = 0; 
	mToken=""; 
	mTokenOrig="";
	mFirstPos = 0; 
	mDistance = 0;
	mDBID = 0;
	mDirty = false;
}

void CToken::IncrFreq(const string& docName) 
{ 
	mFreq++; 
	if (mLastDocFound!=docName) {
		//increment doc frequency only once per document for each token
		mLastDocFound=docName;
		mDocFreq++;
	}
	Dirty(true); 
}

void CToken::IncrFreq()
{
	mFreq++;
	mDocFreq++;

	Dirty(true); 
}

void CToken::AddURLID(long pID)
{
	mURLList.insert(pID);

	Dirty(true); 
}

set<long>& CToken::GetURLList()
{
	return mURLList;
}

// Apply Porter's algorithm to stem it
bool CToken::StemIt() 
{
	bool rc=false;

	try
	{
		if (mToken.length() > 100)
			return rc; // only stem strings that are real words.

		if (mStemmer==NULL)
			mStemmer = create_stemmer();

		memset(mStemmerBuff,0,101);
		int len = min(mToken.size(), 100);
		strncpy(mStemmerBuff, mToken.c_str(), len);

		int res = stem(mStemmer, mStemmerBuff, len-1);

		if (res >= 0) 
		{
			if (res < 200) 
			{
				mStemmerBuff[res+1] = 0;
				mToken = mStemmerBuff;
				rc=true;
			} 
			else 
			{
				printf("*** Error in StemIt() stem() returned Idx out of range =%d\n", res);
			}
		}
	}
	catch(...)
	{
		printf("*** Exception in StemIt: %s\n", mToken.c_str());
	}

	return rc;
}
