#ifndef _PORTERSTEMMER_H
#define _PORTERSTEMMER_H

struct stemmer;

extern struct stemmer * create_stemmer(void);
extern void free_stemmer(struct stemmer * z);

extern int stem(struct stemmer * z, char * b, int k);

#endif
