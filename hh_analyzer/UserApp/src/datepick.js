import Lightpick from 'lightpick'
var picker = new Lightpick({
    field: document.getElementById('f1'),
    secondField: document.getElementById('f2'),
    singleDate: false,
    onSelect: function(start, end){
        var str = '';
        str += start ? start.format('Do MMMM YYYY') + ' to ' : '';
        str += end ? end.format('Do MMMM YYYY') : '...';
        console.log(str);
    }
});
