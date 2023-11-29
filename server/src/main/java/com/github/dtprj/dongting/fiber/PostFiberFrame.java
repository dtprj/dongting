package com.github.dtprj.dongting.fiber;

public abstract class PostFiberFrame<O1, O2> extends FiberFrame<O2> {

    private final FiberFrame<O1> subFrame;

    public PostFiberFrame(FiberFrame<O1> subFrame) {
        this.subFrame = subFrame;
    }

    @Override
    public final FrameCallResult execute(Void v) {
        return Fiber.call(subFrame, this::postProcess);
    }

    protected abstract FrameCallResult postProcess(O1 result) throws Exception;
}
