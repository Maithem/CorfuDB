package org.corfudb.agent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.utility.JavaModule;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.nio.file.Files;

import static net.bytebuddy.implementation.bytecode.assign.Assigner.Typing.DYNAMIC;
import static net.bytebuddy.matcher.ElementMatchers.is;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.noneOf;

public class Agent {

    public static void premain(final String args, final Instrumentation instrumentation) {
        final File tempFolder;

        try {
            tempFolder= Files.createTempDirectory("agent-bootstrap").toFile();
        } catch (Exception e) {
            System.err.println("[Agent] Cannot create temp folder for bootstrap class instrumentation");
            e.printStackTrace(System.err);
            return;
        }

        new AgentBuilder.Default() //
                .disableClassFormatChanges() //
                .ignore(noneOf(Thread.class)) //
                .with(AgentBuilder.InitializationStrategy.NoOp.INSTANCE) //
                .with(AgentBuilder.RedefinitionStrategy.REDEFINITION) //
                .with(AgentBuilder.TypeStrategy.Default.REDEFINE) //
                .enableBootstrapInjection(instrumentation, tempFolder) //
                .type(is(Thread.class)) //
                .transform(new AgentBuilder.Transformer() {
                    @Override
                    public DynamicType.Builder<?> transform(DynamicType.Builder<?> builder, TypeDescription typeDescription, ClassLoader classLoader, JavaModule javaModule) {
                        return builder.visit(Advice.to(ThreadStartInterceptor.class).on(named("interrupt")));
                    }
                }) //
                .installOn(instrumentation);

        System.out.println("premain: transformed thread classes in " + tempFolder.getAbsolutePath());
    }

    public static void agentmain(String args, Instrumentation inst) {
        premain(args, inst);
    }

    public static class ThreadStartInterceptor {
        @Advice.OnMethodEnter
        static void intercept(
                @Advice.This(typing = DYNAMIC, optional = true) Thread target,
                @Advice.Origin Method method,
                @Advice.AllArguments(readOnly = false, typing = DYNAMIC) Object[] args
        ) {
            StringWriter sw = new StringWriter();
            new Throwable("").printStackTrace(new PrintWriter(sw));
            String stackTrace = sw.toString();
            System.out.println("[Agent] src: " + Thread.currentThread().getName() + " dest: " + target.getName() + " STACK: ");
            System.out.println(sw);
        }
    }
}