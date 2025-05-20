package entao

import io.github.yangentao.sql.pool.HarePool
import io.github.yangentao.sql.pool.LiteSources
import io.github.yangentao.sql.utils.XConfig
import io.github.yangentao.xlog.loge
import kotlin.test.Test
import kotlin.test.assertEquals

class ConfigTest {
    @Test
    fun put() {
        HarePool.pushSource(LiteSources.sqliteMemory())
        val g = XConfig.group("entao")
        g.put("name", "entao yang")
        g.put("age", 99)
        loge(g.getString("name"), g.getInt("age"))
        assertEquals(99, g.getInt("age"))
        assertEquals("entao yang", g.getString("name"))

    }
}


