using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using B3ToStorage.Entities;

namespace B3ToStorage
{
    public class Cotacao
    {
        public Cotacao() { }
        public Cotacao(string linhaB3)
        {
            Ano = Convert.ToInt32(linhaB3.Substring(2, 4));
            Mes = Convert.ToInt32(linhaB3.Substring(6, 2));
            Dia = Convert.ToInt32(linhaB3.Substring(8, 2));
            TipoMercado = (TipoMercado)Convert.ToInt32(linhaB3.Substring(24, 3));
            Ticker = linhaB3.Substring(12, 11).Trim();
            PrecoAbertura = Convert.ToDouble(linhaB3.Substring(56, 13)) / 100;
            PrecoMax = Convert.ToDouble(linhaB3.Substring(69, 13)) / 100;
            PrecoMin = Convert.ToDouble(linhaB3.Substring(82, 13)) / 100;
            PrecoMedio = Convert.ToDouble(linhaB3.Substring(95, 13)) / 100;
            PrecoFechamento = Convert.ToDouble(linhaB3.Substring(108, 13)) / 100;
            NumeroNegocios = Convert.ToInt32(linhaB3.Substring(147, 5));
            VolumeNegociado = Convert.ToDouble(linhaB3.Substring(170, 18)) / 100;
        }

        public int Ano { get; set; }
        public int Mes { get; set; }
        public int Dia { get; set; }
        [CsvHelper.Configuration.Attributes.Ignore]
        public DateTime Data { get => new DateTime(Ano, Mes, Dia); }
        public TipoMercado TipoMercado { get; set; }
        public string Ticker { get; set; }
        public double PrecoAbertura { get; set; }
        public double PrecoMax { get; set; }
        public double PrecoMin { get; set; }
        public double PrecoMedio { get; set; }
        public double PrecoFechamento { get; set; }
        public int NumeroNegocios { get; set; }
        public double VolumeNegociado { get; set; }

    }
}
