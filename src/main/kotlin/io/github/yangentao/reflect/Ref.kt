package io.github.yangentao.reflect

import io.github.yangentao.anno.userName
import kotlin.jvm.internal.CallableReference
import kotlin.jvm.internal.FunctionReference
import kotlin.reflect.*
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.valueParameters
import kotlin.reflect.jvm.javaField
import kotlin.reflect.jvm.javaMethod

typealias Prop = KProperty<*>
typealias PropInt = KProperty<Int>
typealias PropLong = KProperty<Long>
typealias PropString = KProperty<String>

typealias Prop0 = KProperty0<*>
typealias Prop1 = KProperty1<*, *>
typealias KotClass = KClass<*>

fun KFunction<Any?>.description(): String {
    val sb = StringBuilder(128)
    val cls = this.ownerClass
    if (cls != null) {
        sb.append(cls.simpleName)
        sb.append('.')
    }
    sb.append(this.name)
    sb.append("(")
    val pList = this.parameters
    for ((n, p) in pList.withIndex()) {
        if (p.kind != KParameter.Kind.VALUE) continue
        sb.append((p.type.classifier as? KClass<*>)?.simpleName)
        sb.append(" ")
        sb.append(p.name)
        if (n != pList.lastIndex) {
            sb.append(", ")
        }
    }
    sb.append(")")
    return sb.toString()
}

@Suppress("unused")
val <T : Any> KClass<T>.memberPropertiesSorted: List<KProperty1<T, *>>
    get() {
        val map = this.java.fields.withIndex().associate { it.value.name to it.index }
        return this.memberProperties.sortedBy { map[it.name] ?: map[it.name + "$" + "delegate"] }
    }
val <T : Any> KClass<T>.declaredMemberPropertiesSorted: List<KProperty1<T, *>>
    get() {
        val map = this.java.declaredFields.withIndex().associate { it.value.name to it.index }
        return this.declaredMemberProperties.sortedBy { map[it.name] ?: map[it.name + "$" + "delegate"] }
    }

fun KParameter.acceptValue(value: Any): Boolean {
    return value::class.isSubclassOf(this.type.classifier as KClass<*>)
}

fun KParameter.acceptClass(cls: KClass<*>): Boolean {
    return cls.isSubclassOf(this.type.classifier as KClass<*>)
}

fun KClass<*>.createInstanceArgOne(arg: Any): Any? {
    val c = this.constructors.firstOrNull {
        val requredParams = it.valueParams
        requredParams.size == 1 && requredParams.first().acceptValue(arg)
    }
    return c?.call(arg)
}

@Suppress("UNCHECKED_CAST")
fun <T : Any> KClass<*>.createInstanceX(vararg ps: Any): T? {
    val c = this.constructors.firstOrNull {
        var result = true
        val vps = it.valueParams
        if (vps.size == ps.size) {
            for (i in vps.indices) {
                if (!vps[i].acceptValue(ps[i])) {
                    result = false
                    break
                }
            }
        } else {
            result = false
        }
        result
    } ?: return null
    return c.call(*ps) as T
}

val KFunction<*>.paramNames: List<String>
    get() {
        return this.parameters.filter { it.kind == KParameter.Kind.VALUE }.map { it.userName }
    }
val KFunction<*>.valueParams: List<KParameter>
    get() {
        return this.parameters.filter { it.kind == KParameter.Kind.VALUE }
    }

val KType.genericArgs: List<KTypeProjection> get() = this.arguments.filter { it.variance == KVariance.INVARIANT }
val KType.isGeneric: Boolean get() = this.arguments.isNotEmpty()

val KFunction<*>.ownerClass: KClass<*>?
    get() {
        if (this is FunctionReference) {
            if (this.boundReceiver != CallableReference.NO_RECEIVER) {
                return this.boundReceiver::class
            }
            val c = this.owner as? KClass<*>
            if (c != null) {
                return c
            }
        } else {
            return this.javaMethod?.declaringClass?.kotlin
        }
        return null
    }
val KFunction<*>.ownerObject: Any?
    get() {
        if (this is FunctionReference) {
            if (this.boundReceiver != CallableReference.NO_RECEIVER) {
                return this.boundReceiver
            }
        }
        return null
    }

val KFunction<*>.firstParamName: String?
    get() {
        return this.valueParameters.firstOrNull()?.userName
    }

val KProperty<*>.ownerClass: KClass<*>?
    get() {
        if (this is CallableReference) {
            if (this.boundReceiver != CallableReference.NO_RECEIVER) {
                return this.boundReceiver::class
            }
            val c = this.owner as? KClass<*>
            if (c != null) {
                return c
            }
        } else {
            return this.javaField?.declaringClass?.kotlin
        }

        return null
    }

val KProperty<*>.ownerObject: Any?
    get() {
        if (this is CallableReference) {
            if (this.boundReceiver != CallableReference.NO_RECEIVER) {
                return this.boundReceiver::class
            }
        }
        return null
    }

val KProperty<*>.returnClass: KClass<*> get() = this.returnType.classifier as KClass<*>

fun KProperty<*>.getPropValue(inst: Any? = null): Any? {
    if (this.getter.parameters.isEmpty()) {
        return this.getter.call()
    }
    val v = this.getter.call(inst)
    if (v != null) return v
    return this.decodeValue(null)
}

fun KMutableProperty<*>.setPropValue(inst: Any, value: Any?) {
    try {
        this.setter.call(inst, this.decodeValue(value))
    } catch (t: Throwable) {
        if (value != null) {
            println("Error setValue: ${this.returnType}" + this.userName + "." + this.name + " value type:" + value::class + ", value=" + value?.toString())
        }
        t.printStackTrace()
        throw t
    }
}

val KProperty<*>.isPublic: Boolean get() = this.visibility == KVisibility.PUBLIC
val KFunction<*>.isPublic: Boolean get() = this.visibility == KVisibility.PUBLIC

fun KFunction<*>.invokeMap(inst: Any? = null, nameMap: Map<String, Any?> = emptyMap(), typeMap: Map<KClass<*>, Any?> = emptyMap(), typeList: List<Any> = emptyList()): Any? {
    fun valueParam(paramMap: HashMap<KParameter, Any?>, p: KParameter, value: Any?) {
        if (value == null) {
            if (p.type.isMarkedNullable) {
                paramMap[p] = null
                return
            }
            if (p.isOptional) return
            error("value is null : $p")
        }
        if (p.acceptValue(value)) {
            paramMap[p] = value
            return
        }
        paramMap[p] = p.decodeValue(value)
    }

    val typeListMap: Map<KClass<*>, Any?> = if (typeList.isEmpty()) {
        typeMap
    } else {
        val map = LinkedHashMap<KClass<*>, Any?>()
        map.putAll(typeMap)
        for (a in typeList) {
            map[a::class] = a
        }
        map
    }

    val paramMap = HashMap<KParameter, Any?>()
    forParams@
    for (p in this.parameters) {
        when (p.kind) {
            KParameter.Kind.INSTANCE -> paramMap[p] = inst
            KParameter.Kind.EXTENSION_RECEIVER -> paramMap[p] = inst
            KParameter.Kind.VALUE -> {
                //先根据参数名匹配
                val nv = nameMap[p.name!!]
                if (nv != null) {
                    valueParam(paramMap, p, nv)
                    continue
                }
                val pclass = p.type.classifier as KClass<*>
                //精确匹配类型
                val tv = typeListMap[pclass]
                if (tv != null) {
                    paramMap[p] = tv
                    continue
                }
                //子类
                for (e in typeListMap.entries) {
                    if (e.key.isSubclassOf(pclass)) {
                        paramMap[p] = e.value
                        continue@forParams
                    }
                }

                valueParam(paramMap, p, null)
            }
        }
    }
    return this.callBy(paramMap)
}