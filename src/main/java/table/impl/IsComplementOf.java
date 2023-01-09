package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsComplementOf extends CassandraTable {

    public IsComplementOf(Database database) {
        super("IsComplementOf", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("key", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("complement", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String key;
        private final Integer num;
        private final String complement;

        public Row(String key, String complement) {
            this.key = key;
            this.num = 1;
            this.complement = complement;
        }

        public Row(String key, Integer num, String complement) {
            this.key = key;
            this.num = num;
            this.complement = complement;
        }

        public String getKey() {
            return key;
        }

        public Integer getNum() {
            return num;
        }

        public String getComplement() {
            return complement;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsComplementOf'{'key={0}, num=''{1}'', complement={2}'}'", key, num, complement);
        }
    }

}
