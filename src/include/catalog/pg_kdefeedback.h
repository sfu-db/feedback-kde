/*
 *
 * pg_kdefeedback.h
 *    definition of system catalogue tables for the KDE feedback collection.
 *
 * src/include/catalog/pg_kdefeedback.h
 *
 * NOTES
 *    the genbki.pl script reads this file and generates .bki
 *    information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_KDEFEEDBACK_H_
#define PG_KDEFEEDBACK_H_

#include "catalog/genbki.h"

/*
 * Definition of the KDE feedback table.
 */
#define KdeFeedbackRelationID  3779

CATALOG(pg_kdefeedback,3779) BKI_WITHOUT_OIDS
{
  int8    timestamp;
  Oid     table;
  int32   columns;
#ifdef CATALOG_VARLEN
  bytea   ranges;
#endif
  float8  alltuples;
  float8  qualifiedtuples;
} FormData_pg_kdefeedback;

/* ----------------
 *    Form_pg_kdefeedback corresponds to a pointer to a tuple with
 *    the format of pg_kdefeedback relation.
 * ----------------
 */
typedef FormData_pg_kdefeedback *Form_pg_kdefeedback;

/* ----------------
 *    compiler constants for pg_kdefeedback
 * ----------------
 */
#define Natts_pg_kdefeedback                  6
#define Anum_pg_kdefeedback_timestamp         1
#define Anum_pg_kdefeedback_relid             2
#define Anum_pg_kdefeedback_columns           3
#define Anum_pg_kdefeedback_ranges            4
#define Anum_pg_kdefeedback_all_tuples        5
#define Anum_pg_kdefeedback_qualified_tuples  6


#endif /* PG_KDEFEEDBACK_H_ */
