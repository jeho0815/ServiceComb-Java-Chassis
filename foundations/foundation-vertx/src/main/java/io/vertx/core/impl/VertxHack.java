package io.vertx.core.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.util.ReflectionUtils;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;

public final class VertxHack {

  private static Constructor<Vertx> constructor;

  static {
    init();
  }

  @SuppressWarnings("unchecked")
  private static synchronized void init() {
    if (constructor != null) {
      return;
    }
    try {
      ClassPool classPool = ClassPool.getDefault();
      URL url = classPool.find("io.vertx.core.impl.VertxImpl");
      System.out.println(url);
      CtClass ctVertxImpl = classPool.get("io.vertx.core.impl.VertxImpl");
      CtConstructor[] ctConstructors = ctVertxImpl.getDeclaredConstructors();
      for (CtConstructor constructor : ctConstructors) {
        constructor.setModifiers(constructor.getModifiers() | Modifier.PUBLIC);
      }
      CtClass ctVertxImplEx = ctVertxImpl.makeNestedClass("VertxImplEx", true);
      ctVertxImplEx.setSuperclass(ctVertxImpl);

      CtConstructor ctConstructor = new CtConstructor(
          classPool.get(new String[] {String.class.getName(), VertxOptions.class.getName()}), ctVertxImplEx);
      ctConstructor
          .setBody("super($2, io.vertx.core.net.impl.transport.Transport.transport($2.getPreferNativeTransport()));");
      ctVertxImplEx.addConstructor(ctConstructor);

      CtClass automicLongClass = classPool.getOrNull(AtomicLong.class.getName());
      CtField ctField = new CtField(automicLongClass, "eventLoopContextCreated", ctVertxImplEx);
      ctVertxImplEx.addField(ctField, CtField.Initializer.byExpr("new java.util.concurrent.atomic.AtomicLong()"));

      CtMethod createEventLoopContextMethod = CtNewMethod
          .copy(ctVertxImplEx.getSuperclass().getDeclaredMethod("createEventLoopContext"), ctVertxImplEx, null);
      createEventLoopContextMethod.setBody("{$0.eventLoopContextCreated.incrementAndGet();\n"
          + "    return super.createEventLoopContext($1, $2, $3, $4);}");
      ctVertxImplEx.addMethod(createEventLoopContextMethod);

      CtMethod getEventLoopContextCreatedCountMethod = new CtMethod(CtClass.longType, "", null, ctVertxImplEx);
      getEventLoopContextCreatedCountMethod.setBody("{return eventLoopContextCreated.get();}");
      ctVertxImplEx.addMethod(getEventLoopContextCreatedCountMethod);

      ctVertxImpl.toClass();
      Class<?> cls = ctVertxImplEx.toClass();
      constructor = (Constructor<Vertx>) cls.getConstructor(String.class, VertxOptions.class);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  public static Vertx getVertx(String name, VertxOptions vertxOptions) {
    try {
      VertxImpl vertx = (VertxImpl) constructor.newInstance(name, vertxOptions);

      Field field = ReflectionUtils.findField(VertxImpl.class, "eventLoopThreadFactory");
      field.setAccessible(true);
      VertxThreadFactory eventLoopThreadFactory = (VertxThreadFactory) ReflectionUtils.getField(field, vertx);

      field = ReflectionUtils.findField(eventLoopThreadFactory.getClass(), "prefix");
      field.setAccessible(true);

      String prefix = (String) ReflectionUtils.getField(field, eventLoopThreadFactory);
      ReflectionUtils.setField(field, eventLoopThreadFactory, name + "-" + prefix);

      Method initMethod = ReflectionUtils.findMethod(VertxImpl.class, "init");
      initMethod.setAccessible(true);
      initMethod.invoke(vertx);
      return vertx;
    } catch (Exception e) {
      throw new RuntimeException("Get VertxImpl extention object error, maybe some internal error!", e);
    }
  }

  public static void main(String[] args) {
    getVertx("test", new VertxOptions());
  }
}
