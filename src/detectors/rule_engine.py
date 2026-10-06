"""Motor de regras para detecção de fraudes."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any


@dataclass
class Regra:
    """Definição de uma regra de fraude."""

    nome: str
    descricao: str
    ativa: bool = True
    peso: float = 1.0


@dataclass
class AlertasFraude:
    """Armazena alertas de fraude detectados."""

    transacao_id: str
    regras_ativadas: list[str]
    score: float
    timestamp: str


def decidir_alerta(
    regras: list[str],
    score_regras: float,
    ml_prob: float | None,
    limiar_ml: float = 0.5,
) -> tuple[bool, float]:
    """Combina regras + ML numa decisão (R2).

    Dispara se alguma regra ativou OU se P(fraude) do ML >= limiar.
    ML ausente (None, ex. sem modelo treinado) nunca dispara sozinho:
    o fallback é regras. Score soma as duas evidências.

    Returns:
        (dispara, score_total)
    """
    ml_hit = ml_prob is not None and ml_prob >= limiar_ml
    dispara = bool(regras) or ml_hit
    score = score_regras + (ml_prob if ml_prob is not None else 0.0)
    return dispara, score


class RuleEngine:
    """Motor de regras para detecção de fraudes."""

    def __init__(self, limiar_alerta: float | None = None) -> None:
        """Inicializa o motor de regras.

        Args:
            limiar_alerta: score mínimo p/ gerar alerta (R7). Sinais fracos
                sozinhos (horario 1.0, dispositivo 1.2, valor 1.5) só somam;
                disparam combinados (ex. 1.0+1.2=2.2) ou com sinal forte
                (velocidade 2.0, geografico 2.5). Env LIMIAR_ALERTA, default 2.0.
        """
        import os

        self.limiar_alerta = (
            limiar_alerta if limiar_alerta is not None else float(os.getenv("LIMIAR_ALERTA", "2.0"))
        )
        self.regras: list[Regra] = [
            Regra(
                nome="velocidade",
                descricao="Mais de 5 transações em 1 minuto",
                peso=2.0,
            ),
            Regra(
                nome="valor_alto",
                descricao="Transações acima de R$ 10.000",
                peso=1.5,
            ),
            Regra(
                nome="geografico",
                descricao="Transações de países diferentes em 5 minutos",
                peso=2.5,
            ),
            Regra(
                nome="horario_incomum",
                descricao="Transações entre 00:00 e 05:00",
                peso=1.0,
            ),
            Regra(
                nome="dispositivo_novo",
                descricao="Transação de dispositivo não registrado",
                peso=0.8,  # R7: sinal mais fraco; sozinho ou +madrugada não alerta
            ),
        ]

        # Histórico de transações por usuário
        self.historico: dict[int, list[dict[str, Any]]] = defaultdict(list)

        # Dispositivos conhecidos por usuário
        self.dispositivos_conhecidos: dict[int, set[str]] = defaultdict(set)

        # Alertas gerados
        self.alertas: list[AlertasFraude] = []

    def _verificar_velocidade(self, transacao: dict[str, Any]) -> str | None:
        """Verifica se há muitas transações em curto período.

        Args:
            transacao: Dados da transação

        Returns:
            Nome da regra se ativada, None caso contrário
        """
        user_id = transacao["user_id"]
        timestamp_atual = datetime.fromisoformat(transacao["timestamp"])
        um_minuto_atras = timestamp_atual - timedelta(minutes=1)

        # Conta transações no último minuto
        transacoes_recentes = [
            t
            for t in self.historico[user_id]
            if datetime.fromisoformat(t["timestamp"]) > um_minuto_atras
        ]

        if len(transacoes_recentes) >= 5:
            return "velocidade"

        return None

    def _verificar_valor_alto(self, transacao: dict[str, Any]) -> str | None:
        """Verifica se o valor da transação é muito alto.

        Args:
            transacao: Dados da transação

        Returns:
            Nome da regra se ativada, None caso contrário
        """
        if transacao["valor"] > 10000:
            return "valor_alto"

        return None

    def _verificar_geografico(self, transacao: dict[str, Any]) -> str | None:
        """Verifica transações de locais distantes em curto período.

        Args:
            transacao: Dados da transação

        Returns:
            Nome da regra se ativada, None caso contrário
        """
        user_id = transacao["user_id"]
        timestamp_atual = datetime.fromisoformat(transacao["timestamp"])
        cinco_minutos_atras = timestamp_atual - timedelta(minutes=5)

        pais_atual = transacao.get("localizacao", {}).get("pais", "")

        # Verifica transações nos últimos 5 minutos
        transacoes_recentes = [
            t
            for t in self.historico[user_id]
            if datetime.fromisoformat(t["timestamp"]) > cinco_minutos_atras
        ]

        for t in transacoes_recentes:
            pais_anterior = t.get("localizacao", {}).get("pais", "")
            if pais_anterior and pais_atual and pais_anterior != pais_atual:
                return "geografico"

        return None

    def _verificar_horario_incomum(self, transacao: dict[str, Any]) -> str | None:
        """Verifica se a transação é em horário incomum.

        Args:
            transacao: Dados da transação

        Returns:
            Nome da regra se ativada, None caso contrário
        """
        timestamp = datetime.fromisoformat(transacao["timestamp"])
        hora = timestamp.hour

        if 0 <= hora < 5:
            return "horario_incomum"

        return None

    def _verificar_dispositivo_novo(self, transacao: dict[str, Any]) -> str | None:
        """Verifica se o dispositivo é novo para o usuário.

        Args:
            transacao: Dados da transação

        Returns:
            Nome da regra se ativada, None caso contrário
        """
        user_id = transacao["user_id"]
        dispositivo = transacao.get("dispositivo", "")

        if dispositivo and dispositivo not in self.dispositivos_conhecidos[user_id]:
            return "dispositivo_novo"

        return None

    def verificar_transacao(self, transacao: dict[str, Any]) -> AlertasFraude | None:
        """Verifica uma transação contra todas as regras.

        Args:
            transacao: Dados da transação

        Returns:
            AlertasFraude se alguma regra foi ativada, None caso contrário
        """
        # Fronteira anti-mistura naive/aware (R3): normaliza antes de comparar.
        try:
            ts = datetime.fromisoformat(transacao["timestamp"])
            if ts.tzinfo is None:
                transacao = {
                    **transacao,
                    "timestamp": ts.replace(tzinfo=UTC).isoformat(),
                }
        except (KeyError, ValueError):
            pass

        regras_ativadas: list[str] = []
        score_total = 0.0

        # Executa todas as regras ativas
        for regra in self.regras:
            if not regra.ativa:
                continue

            regra_func = getattr(self, f"_verificar_{regra.nome}", None)
            if regra_func:
                resultado = regra_func(transacao)
                if resultado:
                    regras_ativadas.append(regra.nome)
                    score_total += regra.peso

        # Atualiza histórico
        user_id = transacao["user_id"]
        self.historico[user_id].append(transacao)

        # Registra dispositivo conhecido
        dispositivo = transacao.get("dispositivo", "")
        if dispositivo:
            self.dispositivos_conhecidos[user_id].add(dispositivo)

        # Mantém apenas últimas 100 transações no histórico
        if len(self.historico[user_id]) > 100:
            self.historico[user_id] = self.historico[user_id][-100:]

        # R7: alerta só com evidência suficiente; sinal fraco sozinho soma
        # score mas não dispara (ex. madrugada ou 1º uso isolados).
        if regras_ativadas and score_total >= self.limiar_alerta:
            alerta = AlertasFraude(
                transacao_id=transacao["transaction_id"],
                regras_ativadas=regras_ativadas,
                score=score_total,
                timestamp=transacao["timestamp"],
            )
            self.alertas.append(alerta)
            return alerta

        return None

    def obter_estatisticas(self) -> dict[str, Any]:
        """Retorna estatísticas do motor de regras."""
        total_regras_ativadas: dict[str, int] = defaultdict(int)

        for alerta in self.alertas:
            for regra in alerta.regras_ativadas:
                total_regras_ativadas[regra] += 1

        return {
            "total_alertas": len(self.alertas),
            "regras_ativadas": dict(total_regras_ativadas),
            "regras_configuradas": len(self.regras),
            "usuarios_monitorados": len(self.historico),
        }


def main() -> None:
    """Função principal para demonstração."""
    engine = RuleEngine()

    # Exemplo de transação
    transacao_exemplo = {
        "transaction_id": "abc-123",
        "user_id": 42,
        "valor": 15000.00,
        "timestamp": datetime.now(UTC).isoformat(),
        "localizacao": {"cidade": "São Paulo", "pais": "BR"},
        "dispositivo": "mobile",
        "categoria": "compras",
    }

    print("🔍 Verificando transação...")
    resultado = engine.verificar_transacao(transacao_exemplo)

    if resultado:
        print("🚨 FRAUDE DETECTADA!")
        print(f"   ID: {resultado.transacao_id}")
        print(f"   Regras: {', '.join(resultado.regras_ativadas)}")
        print(f"   Score: {resultado.score:.2f}")
    else:
        print("✅ Transação aprovada")

    print(f"\n📊 Estatísticas: {engine.obter_estatisticas()}")


if __name__ == "__main__":
    main()
