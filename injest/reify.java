//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.IObj;
import clojure.lang.IPersistentMap;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Tuple;
import clojure.lang.Var;
import com.google.cloud.functions.CloudEventsFunction;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;

public final class injest$reify__948 implements CloudEventsFunction, IObj {
    final IPersistentMap __meta;
    public static final Var const__0 = (Var)RT.var("clojure.pprint", "pprint");
    public static final AFn const__1 = (AFn)Symbol.intern((String)null, "this");
    public static final AFn const__2 = (AFn)Symbol.intern((String)null, "cloudevent");
    public static final Var const__3 = (Var)RT.var("clojure.core", "slurp");
    public static final Var const__4 = (Var)RT.var("injest", "-main");

    public injest$reify__948(IPersistentMap var1) {
        this.__meta = var1;
    }

    public injest$reify__948() {
        this((IPersistentMap)null);
    }

    public IPersistentMap meta() {
        return this.__meta;
    }

    public IObj withMeta(IPersistentMap var1) {
        return new injest$reify__948(var1);
    }

    public void accept(CloudEvent cloudevent) throws Exception {
        ((IFn)const__0.getRawRoot()).invoke(Tuple.create(const__1, this));
        ((IFn)const__0.getRawRoot()).invoke(Tuple.create(const__2, cloudevent));
        IFn var10000 = (IFn)const__3.getRawRoot();
        CloudEvent var10001 = cloudevent;
        cloudevent = null;
        Object event = var10000.invoke(((CloudEventData)((CloudEvent)var10001).getData()).toBytes());
        var10000 = (IFn)const__4.getRawRoot();
        Object var10002 = event;
        event = null;
        injest$reify__948 var3 = null;
        var10000.invoke("slack", var10002);
    }
}
