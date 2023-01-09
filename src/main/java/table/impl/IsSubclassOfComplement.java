package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

public class IsSubclassOfComplement extends CassandraTable {

    public IsSubclassOfComplement(Database database) {
        super("IsSubclassOfComplement", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("key", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("complement", DataTypes.TEXT);
    }

}
