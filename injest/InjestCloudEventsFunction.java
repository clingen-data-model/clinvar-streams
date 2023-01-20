//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package injest;

import clojure.lang.IFn;
import clojure.lang.Util;
import clojure.lang.Var;
import com.google.cloud.functions.CloudEventsFunction;
import io.cloudevents.CloudEvent;

public class InjestCloudEventsFunction implements CloudEventsFunction {
    private static final Var equals__var = Var.internPrivate("injest", "InjestCloudEventsFunction-equals");
    private static final Var toString__var = Var.internPrivate("injest", "InjestCloudEventsFunction-toString");
    private static final Var hashCode__var = Var.internPrivate("injest", "InjestCloudEventsFunction-hashCode");
    private static final Var clone__var = Var.internPrivate("injest", "InjestCloudEventsFunction-clone");
    private static final Var accept__var = Var.internPrivate("injest", "InjestCloudEventsFunction-accept");

    static {
        Util.loadWithClass("/injest", InjestCloudEventsFunction.class);
    }

    public InjestCloudEventsFunction() {
    }

    public boolean equals(Object var1) {
        Var var10000 = equals__var;
        Object var2 = var10000.isBound() ? var10000.get() : null;
        return var2 != null ? (Boolean)((IFn)var2).invoke(this, var1) : super.equals(var1);
    }

    public String toString() {
        Var var10000 = toString__var;
        Object var1 = var10000.isBound() ? var10000.get() : null;
        return var1 != null ? (String)((IFn)var1).invoke(this) : super.toString();
    }

    public int hashCode() {
        Var var10000 = hashCode__var;
        Object var1 = var10000.isBound() ? var10000.get() : null;
        return var1 != null ? ((Number)((IFn)var1).invoke(this)).intValue() : super.hashCode();
    }

    public Object clone() {
        Var var10000 = clone__var;
        Object var1 = var10000.isBound() ? var10000.get() : null;
        return var1 != null ? ((IFn)var1).invoke(this) : super.clone();
    }

    public void accept(CloudEvent var1) {
        Var var10000 = accept__var;
        Object var2 = var10000.isBound() ? var10000.get() : null;
        if (var2 != null) {
            ((IFn)var2).invoke(this, var1);
        } else {
            throw new UnsupportedOperationException("accept (injest/InjestCloudEventsFunction-accept not defined?)");
        }
    }
}
