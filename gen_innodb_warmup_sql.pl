#! perl

use strict;
use warnings;
use utf8;
use DBI;
use Getopt::Long;
use IO::Handle;
use Data::Dumper;

our %OPTIONS = (
    ignore_table => [],
    high_priority_table => [],
    low_priority_table => [],
);

##
# メイン
##
main: {
    GetOptions(
        \%OPTIONS,
        "db:s",
        "dbuser:s",
        "dbpass:s",
        "dbhost:s",
        "ignore_table:s@",
        "high_priority_table:s@",
        "low_priority_table:s@"
    );
    
    my $db = getConnection();
    $db->{AutoCommit} = 0;
    my $targetTables = getTargetTables($db);
    my $sql = genInnodbWarmupSql($db, $targetTables);
    print $sql;
    $db->rollback();
}

##
# DBIを返す
##
sub getConnection {
    my $db = DBI->connect(
        sprintf("dbi:mysql:%s:%s", $OPTIONS{'db'} || '', $OPTIONS{'dbhost'} || ''),
        $OPTIONS{'dbuser'},
        $OPTIONS{'dbpass'}
    ) || die $!;
    $db->{RaiseError} = 1;

    # ここがださい(^^;
    $db->do("SET NAMES utf8");
    $db->do("SET SESSION wait_timeout = 1000000");

    return $db;
}

##
# INFORMATION_SCHAMEを読んで以下のようなデータ構造を作り返します
# [
#     {
#         table_name   => テーブル名,
#         primary_keys => [COL1, COL2, COL3],
#         index_keys   => , {
#             インデックス名1 => [COL1, COL2, COL3],
#             インデックス名2 => [COL1, COL2, COL3]
#         },
#     },
#     {}, # 以下同様
#     {}
# ]
# このときオプションで指定されたような感じでソートして返します。
# @return ARRAYREF
##
sub getTargetTables {
    my($db) = @_;
    
    my $targetTables = [];

    my $tableRows = $db->selectall_arrayref(
        q{
            SELECT
                TABLE_NAME
            FROM
                INFORMATION_SCHEMA.TABLES
            WHERE
                TABLE_SCHEMA = ?
        },
        { Slice => {} },
        $OPTIONS{'db'}
    );
    
    foreach my $table (@$tableRows) {
        # print Dumper($table);

        my $keyRows = $db->selectall_arrayref(
            q{
                SELECT
                    INDEX_NAME
                   ,GROUP_CONCAT(COLUMN_NAME) AS COLNAME
                   ,GROUP_CONCAT(SEQ_IN_INDEX) AS SEQNUM
                FROM
                    INFORMATION_SCHEMA.STATISTICS
                WHERE
                    TABLE_SCHEMA = ?
                    AND
                    TABLE_NAME= ?
                GROUP BY
                    INDEX_NAME
            },
            { Slice => {} },
            $OPTIONS{'db'}, $table->{TABLE_NAME}
        );
        
        my $primaryKeys = [];
        my $indexKeys = {};
        foreach my $key (@$keyRows) {
            my $keyColumns = [];

            # 下記配列には "col_a,col_c,col_b" および "2,1,3" のような値が入っている。
            # この後者（1オリジンなので-1する）を$keyColumnsへの挿入順序（添字）とする
            my @colnames = split(/,/, $key->{COLNAME});
            my @seqnums = split(/,/, $key->{SEQNUM});
            for (my $i=0; $i<@colnames; $i++) {
                $keyColumns->[ $seqnums[$i]-1 ] = $colnames[$i];
            }
            
            if ($key->{INDEX_NAME} eq 'PRIMARY') {
                $primaryKeys = $keyColumns;
            } else {
                $indexKeys->{ $key->{INDEX_NAME} } = $keyColumns;
            }
        }

        push(@$targetTables, {
            table_name   => $table->{TABLE_NAME},
            primary_keys => $primaryKeys,
            index_keys   => $indexKeys
        });
    }
    
    $targetTables = _sort_target_table($targetTables);
    
    return $targetTables;
}

##
# InnoDB warmup 用のSQLを作ります。
# @return string SQL
##
sub genInnodbWarmupSql {
    my($db, $targetTables) = @_;

    my $rows = $db->selectall_arrayref(
        q{SELECT SUM(DATA_LENGTH) AS TOTAL_DATA_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?},
        { Slice => {} },
        $OPTIONS{'db'}
    );
    my $totalDataLength = int($rows->[0]->{TOTAL_DATA_LENGTH});
    my $datenow = scalar localtime();
    
    my $sql = '';
    $sql .= "-- --\n";
    $sql .= "-- InnoDB warmup SQL\n";
    $sql .= "--   auto generated at: $datenow\n";
    $sql .= "--   total data length: $totalDataLength\n";
    $sql .= "-- --\n";
    $sql .= "\n";

    foreach my $tgt (@$targetTables) {
        my $tableName = $tgt->{table_name};
        
        my $rows = $db->selectall_arrayref(
            q{SELECT DATA_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?},
            { Slice => {} },
            $OPTIONS{'db'}, $tableName
        );
        my $dataLength = int($rows->[0]->{DATA_LENGTH});
        
        $sql .= "-- table: $tableName, length: $dataLength\n";
        
        if (scalar @{$tgt->{primary_keys}}) {
            my $selectCols = join('+', map{ "LENGTH(`$_`)" }@{$tgt->{primary_keys}});
            $sql .= "SELECT SUM($selectCols) AS `${tableName}_PRIMARY` FROM `$tableName` FORCE INDEX (`PRIMARY`);\n";
        } else {
            warn("$tableName no contains primary key");
            $sql .= "SELECT COUNT(*) AS `${tableName}_COUNT` FROM `$tableName`;\n";
        }

        while (my($idxName,$idxCols) = each(%{$tgt->{index_keys}})) {
            my $selectCols = join('+', map{ "LENGTH(`$_`)" }@$idxCols);
            $sql .= "SELECT SUM($selectCols) AS `${tableName}_$idxName` FROM `$tableName` FORCE INDEX (`$idxName`);\n";
        }

        $sql .= "\n";
    }

    return $sql;
}

##
# ならびかえたり削ったりする
##
sub _sort_target_table {
    my($targetTables) = @_;
    
    foreach my $ignoreTable (@{$OPTIONS{'ignore_table'}}) {
        my $idx = 0;
        foreach my $tgt (@$targetTables) {
            if ($tgt->{table_name} eq $ignoreTable) {
                splice(@$targetTables, $idx, 1);
                last;
            }
            $idx++;
        }
    }

    foreach my $highTable (reverse @{$OPTIONS{'high_priority_table'}}) {
        my $idx = 0;
        foreach my $tgt (@$targetTables) {
            if ($tgt->{table_name} eq $highTable) {
                splice(@$targetTables, $idx, 1);
                push(@$targetTables, $tgt);
                last;
            }
            $idx++;
        }
    }
    
    foreach my $lowTable (reverse @{$OPTIONS{'low_priority_table'}}) {
        my $idx = 0;
        foreach my $tgt (@$targetTables) {
            if ($tgt->{table_name} eq $lowTable) {
                splice(@$targetTables, $idx, 1);
                unshift(@$targetTables, $tgt);
                last;
            }
            $idx++;
        }
    }

    return $targetTables;
}
