package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsEquivalentToSome extends CassandraTable {

    public IsEquivalentToSome(Database database) {
        super("IsEquivalentToSome", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("cls", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("prop", DataTypes.TEXT);
        this.Columns.put("range", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String cls;
        private final Integer num;
        private final String prop;
        private final String range;

        public Row(String cls, Integer num, String prop, String range) {
            this.cls = cls;
            this.num = num;
            this.prop = prop;
            this.range = range;
        }

        public String getCls() {
            return cls;
        }

        public Integer getNum() {
            return num;
        }

        public String getProp() {
            return prop;
        }

        public String getRange() {
            return range;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsEquivalentToSome'{'cls={0}, num=''{1}'', prop={2}, range={3}'}'", cls, num, prop, range);
        }
    }

}
