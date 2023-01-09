package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsEquivalentToUnion extends CassandraTable {

    public IsEquivalentToUnion(Database database) {
        super("IsEquivalentToUnion", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("cls", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("ind1", DataTypes.TEXT);
        this.Columns.put("ind2", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String cls;
        private final Integer num;
        private final String ind1;
        private final String ind2;

        public Row(String cls, Integer num, String ind1, String ind2) {
            this.cls = cls;
            this.num = num;
            this.ind1 = ind1;
            this.ind2 = ind2;
        }

        public String getCls() {
            return cls;
        }

        public Integer getNum() {
            return num;
        }

        public String getInd1() {
            return ind1;
        }

        public String getInd2() {
            return ind2;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsEquivalentToUnion'{'cls={0}, num=''{1}'', ind1={2}, ind2={3}'}'", cls, num, ind1, ind2);
        }

    }

}
