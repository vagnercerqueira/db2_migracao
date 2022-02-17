const formulario = document.getElementById("form_integra");
const btn_submit = document.querySelector("button[type='submit']");

const alerts_succ = document.querySelector(".alert-success");
const alerts_err = document.querySelector(".alert-danger");

const btntimeini = document.querySelector(".timeini");
const btntimefim = document.querySelector(".timefim");
const totreg = document.querySelector(".tot_reg");

formulario.addEventListener("submit", (ev) => {
    valida_periodo = comparaData()
    if (valida_periodo){
        processa_periodo();
    }else{
        alert('Data movimento inicial nao pode ser maior que final!');
    }
    ev.preventDefault()
    return false;
})

const progess_bar = document.querySelector(".md-progress");
function processa_periodo() {
   progess_bar.style.display = "block"
   btn_submit.setAttribute('disabled', true)

   alerts_succ.style.display = "none"
   alerts_err.style.display = "none"

   btntimefim.style.display = "none"
   btntimeini.style.display = "block"
   btntimeini.innerHTML = "HR INICIO: "+pegaHora()

   let data = new FormData(formulario);
   let action = formulario.getAttribute('action');
   fetch(action, {
        method: 'POST',
        credentials: 'same-origin',
        body: data
    }).then(response => {
        return response.json();
    }).then(response => {
        alerts_succ.style.display = "block"
        totreg.innerHTML = response.TOT_INSTRUMENTO_ACAO + ` registros na tabela instrumento acao, `+response.TOT_INSTRUMENTO_DATA + ` registros na tabela instrumento data.`;
    }).finally((r)=>{
        progess_bar.style.display = "none"
        btn_submit.removeAttribute('disabled')

        btntimefim.style.display = "block"
        btntimefim.innerHTML = "HR TERMINO: "+pegaHora()

        console.log(r)
    }).catch(err => {
        alerts_err.style.display = "block"
        alert("Erro na resposta do servidor", err);
    })
}

function comparaData(){
    let dtI = document.querySelector("input[name=data_movimento_inicial]").value
    let dtF = document.querySelector("input[name=data_movimento_final]").value
    dtI = dtI.split('-');
    dtF = dtF.split('-');
    console.log(dtI, dtF)
    let dataI = new Date( (dtI[0]+"/"+dtI[1]+"/"+dtI[2]+"/") );
    let dataF = new Date( (dtF[0]+"/"+dtF[1]+"/"+dtF[2]+"/") );

    if(dataI > dataF){
        return false;
    }
    return true;
}

function pegaHora(){
    let dt = new Date()
    let hr = dt.getHours();
    let min = dt.getMinutes();
    let sec = dt.getSeconds();
    let fullHr = ( hr < 10 ? "0"+hr : hr ) + ":" + ( min < 10 ? "0"+min : min ) + ":" +( sec < 10 ? "0"+sec : sec )
    return fullHr
}

