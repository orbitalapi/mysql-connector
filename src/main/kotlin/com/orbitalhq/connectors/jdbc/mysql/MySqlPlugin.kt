package com.orbitalhq.connectors.jdbc.mysql

import com.mysql.jdbc.Driver
import com.orbitalhq.plugins.Plugin
import com.orbitalhq.plugins.jdbc.DriverProxy
import java.sql.DriverManager

class MySqlPlugin : Plugin {
    override val name: String = "com.orbitalhq.MySql"

    override fun initialize() {
        MySqlDbSupport.register()

    }
}