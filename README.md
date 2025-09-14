## SQL

SQL, ORM, Kotlin

·

    @Action
    fun one(topicId: Long): JsonResult {
        val b = Topics.one(Topics::id EQ topicId, Topics::userId EQ accountID) ?: return JsonFailed()
        return b.jsonResult()
    }

    @Action
    fun list(): JsonResult {
        val ls = Topics.list(Topics::userId EQ accountID) { ORDER_BY(Topics::deadTime.DESC) }
        return ls.jsonResult()
    }

    @Action
    fun repliesOf(topicId: Long): JsonResult {
        if (!Topics.exists(Topics::id EQ topicId, Topics::userId EQ accountID)) return JsonFailed()
        val ls = Replices.list(Replices::topicId EQ topicId)
        return ls.jsonResult()
    }
·

