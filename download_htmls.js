(async () => {
  const url    = window.location.href.split('?')[0];
  const formEl = document.querySelector('form');
  const deptSelect  = document.getElementById('_ctl0_ContentPlaceHolder1_cmbDepartamento');
  const nivelSelect = document.getElementById('_ctl0_ContentPlaceHolder1_cmbNivel');

  // Sólo nos interesa “diversificado” para Nivel:
  const nivelValue = '46';

  // Construimos la lista de departamentos:
  const departamentos = Array.from(deptSelect.options)
    .filter(o => o.value !== 'SELECCIONE UNO')
    .map(o => ({ value: o.value, text: o.text.trim() }));

  for (const dept of departamentos) {
    // 1) Construir FormData sobre el form original
    const fd = new FormData(formEl);

    // 2) Ajustamos los campos que cambian
    fd.set('_ctl0:ContentPlaceHolder1:cmbDepartamento', dept.value);
    fd.set('_ctl0:ContentPlaceHolder1:cmbNivel', nivelValue);

    // 3) Simulamos el clic sobre el botón de Consulta
    //    (coordenadas arbitrarias dentro del botón, e.g. x=1,y=1)
    fd.set('_ctl0:ContentPlaceHolder1:IbtnConsultar.x', '1');
    fd.set('_ctl0:ContentPlaceHolder1:IbtnConsultar.y', '1');

    // 4) Enviamos el POST
    const resp = await fetch(url, {
      method: 'POST',
      body: fd
    });

    // 5) Descargamos el HTML resultante
    const html = await resp.text();
    const blob = new Blob([html], { type: 'text/html' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `busqueda_${dept.text.replace(/\s+/g,'_')}.html`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);

    // 6) Breve pausa para no saturar
    await new Promise(r => setTimeout(r, 1500));
  }

  console.log('✅ Descargas completadas con botón Consultar pulsado');
})()
.catch(err => console.error('❌ Error en descargas:', err));
