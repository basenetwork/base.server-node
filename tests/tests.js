var countTests = 0;

module.exports = {
    start: function(tests) {
        console.log("-------------------------------------------------------------");
        console.log("Testing started at " + new Date());
        var passed = 0, failed = 0;
        for(var name in tests) {
            var fn = tests[name];
            if(typeof fn !== "function") continue;
            try
            {
                process.stdout.write(".");
                fn.call(tests);
                passed++;
            } catch(e) {
                failed++;
                console.log("\nFAIL. "+ name, "\nError:\n", e, "\n\n");
            }
        }
        console.log("\n\n"+(failed? "FAIL" : "OK")+ ":", passed, "passed,", failed, "failed\n");
    },

    equal: function(expected, value) {
        expected = JSON.stringify(expected);
        value = JSON.stringify(value);
        if(expected !== value) {
            throw "\tExpected: "+expected + "\n\tActual:   "+value;
        }
        return this;
    }
};