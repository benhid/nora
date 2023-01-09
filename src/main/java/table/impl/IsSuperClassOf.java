package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

public class IsSuperClassOf extends CassandraTable {

    public IsSuperClassOf(Database database) {
        super("IsSuperClassOf", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("cls", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("subclass", DataTypes.TEXT);
    }

}
