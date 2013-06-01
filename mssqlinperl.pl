#!/usr/bin/perl

use strict;
use Data::Dumper;
use DBI;

my $dbname = "testdb";

my $data_source = "dbi:Sybase:server=192.168.5.104";
my $dbh = DBI->connect($data_source, "abcd123", "abcd123")
  or die "Can't connect to $data_source: $DBI::errstr";


($dbh->do("use $dbname") != -2)
    or die "Cannot switch to $dbname\n";


my $sth = $dbh->prepare( q{
      SELECT CONVERT (VARCHAR (50), o.[userguid]) AS UserGuid,
              CONVERT (VARCHAR (50), [o].[dcreated]) AS [Transaction date & time],
              (SELECT TOP 1 datediff(day,ISNULL([DateModified], [DateCreated]),GETDATE()) FROM [dbo].[UserSessions] us WHERE [us].[UserId] = o.[UserId] AND us.siteid = 1 ORDER BY dcreated
               desc) AS 'LoginDate',
              [o].[ordertotalamount]                           AS [Order Total Amount],
              (SELECT Sum(Isnull(so.suborderprice, 0))
               FROM   suborders AS so
               WHERE  so.orderguid = o.orderguid)              AS [Product Amount],
              CONVERT(float,REPLACE([o].[discountamount],',',''))                             AS [discount given],
              (SELECT CONVERT(float,REPLACE(CONVERT(float,REPLACE([o].[discountamount],',','')),',','')) / Sum(Isnull(so.suborderprice, 0))
               FROM   suborders AS so
               WHERE  so.orderguid = o.orderguid)              AS [Percent],
              CASE
                WHEN (SELECT CONVERT(float,REPLACE([o].[discountamount],',','')) / Sum(Isnull(so.suborderprice, 0))
                      FROM   suborders AS so
                      WHERE  so.orderguid = o.orderguid) BETWEEN 0.7501 AND 0.99 THEN '75-99%'
                WHEN (SELECT CONVERT(float,REPLACE([o].[discountamount],',','')) / Sum(Isnull(so.suborderprice, 0))
                      FROM   suborders AS so
                      WHERE  so.orderguid = o.orderguid) BETWEEN 0.5001 AND 0.75 THEN '51-75%'
                WHEN (SELECT CONVERT(float,REPLACE([o].[discountamount],',','')) / Sum(Isnull(so.suborderprice, 0))
                      FROM   suborders AS so
                      WHERE  so.orderguid = o.orderguid) BETWEEN 0.2501 AND 0.50 THEN '25-50%'
                WHEN (SELECT CONVERT(float,REPLACE([o].[discountamount],',','')) / Sum(Isnull(so.suborderprice, 0))
                      FROM   suborders AS so
                      WHERE  so.orderguid = o.orderguid) BETWEEN 0.0001 AND 0.25 THEN '1-25%'
                ELSE '100%'
              END                                              AS [DiscountPercent],
              CONVERT (VARCHAR (50), [o].[currency]) AS Currency,
              CONVERT (VARCHAR (50), [o].[ordercode]) AS Ordercode,
              CASE
                WHEN (SELECT Count(1)
                      FROM   [dbo].[orderpromos] AS op WITH (nolock)
                      WHERE  op.orderguid = [o].[orderguid]) > 0 THEN
                  CASE
                    WHEN (SELECT Count(1)
                          FROM   [dbo].[orderpromos] AS op WITH (nolock)
                          WHERE  op.orderguid = [o].[orderguid]) = 1 THEN (SELECT TOP 1 [acp].[partnername] + '_'
                                                                                        + [asp].[subpartnername]
                                                                           FROM   [dbo].[orderpromos] AS op WITH (nolock)
                                                                                  INNER JOIN [dbo].[admincenterpromos] AS ap WITH (nolock)
                                                                                          ON [ap].[promoid] = [op].[promoid]
                                                                                  INNER JOIN [dbo].[admincentersubpartner] AS asp WITH (nolock)
                                                                                          ON [ap].[subpartnerid] = [asp].[subpartnerid]
                                                                                  INNER JOIN [dbo].[admincenterpartner] AS acp WITH (nolock)
                                                                                          ON [asp].[partnerid] = [acp].[partnerid]
                                                                           WHERE  op.orderguid = [o].[orderguid])
                    ELSE 'MultiPromo'
                  END
                ELSE ''
              END                                              AS 'Partner_Subpartner',
              CASE
                WHEN (SELECT Count(1)
                      FROM   [dbo].[suborders] AS so WITH (nolock)
                      WHERE  [so].[orderguid] = [o].[orderguid]) = 1 THEN
                  CASE
                    WHEN (SELECT Count(1)
                          FROM   [dbo].[suborders] AS so WITH (nolock)
                          WHERE  [so].[orderguid] = o.orderguid
                                 AND so.printtypecode IS NOT NULL) = 1 THEN (SELECT TOP 1 [pt].[printtypename]
                                                                             FROM   [dbo].[suborders] AS so
                                                                                    INNER JOIN [dbo].[printtypes] AS pt WITH (nolock)
                                                                                            ON [pt].[printtypecode] = [so].[printtypecode]
                                                                                               AND [so].[orderguid] = o.orderguid)
                    ELSE (SELECT TOP 1 [pt].[premiumtypedescription]
                          FROM   [dbo].[suborders] AS so
                                 INNER JOIN [dbo].[premiumtypecode] AS pt WITH (nolock)
                                         ON pt.[premiumtypecodeid] = [so].[premiumtypecode]
                                            AND [so].[orderguid] = [o].[orderguid])
                  END
                ELSE 'MultiProduct'
              END                                              AS 'Product Category',

              (SELECT TOP 1 oa.[country] + '_' + oa.[state] + '_' + [oa].[city]
               FROM   [dbo].[orderaddress] AS oa
               WHERE  [oa].[orderguid] = [o].[orderguid])      AS 'Location of Order'
FROM   orders AS o WITH (nolock)
WHERE  EXISTS (SELECT 1
               FROM   [dbo].[ordereventlog] AS ol WITH (nolock)
               WHERE  ol.[orderguid] = [o].[orderguid]
                      AND [ol].[ordereventid] = 200)
       AND NOT EXISTS (SELECT 1
                       FROM   [dbo].[ordereventlog] AS ol WITH (nolock)
                       WHERE  ol.[orderguid] = [o].[orderguid]
                              AND [ol].[ordereventid] = 295)
}) or die "Can't prepare statement: $DBI::errstr";

my $rc = $sth->execute
  or die "Can't execute statement: $DBI::errstr";

print "Query will return $sth->{NUM_OF_FIELDS} fields.\n\n";
print "\n";


open (MYFILE, '>data.txt');
my @row;
my $str;
$str = join(',', @{ $sth->{NAME} });
print MYFILE $str . "\n";
while ((@row) = $sth->fetchrow_array) {
  $str = join(',',@row);
  print MYFILE $str . "\n";
}
# check for problems which may have terminated the fetch early
close (MYFILE);
die $sth->errstr if $sth->err;

$dbh->disconnect;
