package com.github.dtprj.dongting.fiber;

public abstract class PostFiberFrame<I, O1, O2> extends FiberFrame<I, O2> {

    private final FiberFrame<I, O1> subFrame;

    public PostFiberFrame(FiberFrame<I, O1> subFrame) {
        this.subFrame = subFrame;
    }

    @Override
    public final FrameCallResult execute(I input) {
        return call(input, subFrame, this::postProcess);
    }

    protected abstract FrameCallResult postProcess(O1 result) throws Exception;
}
