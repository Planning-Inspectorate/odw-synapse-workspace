module.exports = async function(context, mySbMsg) {
    context.bindings.output = mySbMsg;
    context.log(`New Message: ${mySbMsg}`);
};