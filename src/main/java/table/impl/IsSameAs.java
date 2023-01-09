package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsSameAs extends CassandraTable {

    public IsSameAs(Database database) {
        super("IsSameAs", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("ind", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("same", DataTypes.TEXT);
    }


    public static class Row implements Serializable {

        private final String ind;
        private final Integer num;
        private final String same;

        public Row(String ind, Integer num, String same) {
            this.ind = ind;
            this.num = num;
            this.same = same;
        }

        public String getInd() {
            return ind;
        }

        public Integer getNum() {
            return num;
        }

        public String getSame() {
            return same;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsSameAs'{'ind={0}, num=''{1}'', same={1}'}'", ind, num, same);
        }

    }

}
